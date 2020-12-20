package com.zll.kafka.delay;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 此类主要功能为
 * 1.添加指定时间的延时任务，可在此类中实现自己的业务逻辑
 * 2.停止运行，包含强制停止和任务完成后停止
 * 3.查看待执行任务数量
 */
public class TimingWheel {
    //时间轮的默认长度
    private static final int STATIC_WHEEL_SIZE=64;

    //数组作为时间轮
    private Object[] timeWheel;
    private int wheelSize;

    //线程池
    private ExecutorService executorService;

    //时间轮中任务数量
    //AtomicBiInteger提供原子操作
    private AtomicInteger taskCount=new AtomicInteger();

    //时间轮停止标识
    private volatile boolean stop=false;
    //条件锁, 用于stop
    private Lock lock=new ReentrantLock();
    private Condition condition = lock.newCondition();




    //使用原子类,初始化只需要一个线程，确定只一次初始化启动
    private volatile AtomicBoolean start=new AtomicBoolean(false);
    //指针
    private AtomicInteger tick=new AtomicInteger();
    //任务id
    private AtomicInteger taskId=new AtomicInteger();
    //按id查找任务
    private Map<Integer, Task> taskMap =new HashMap<>();



    //默认构造
    public TimingWheel(ExecutorService executorService){
        this.executorService=executorService;
        this.wheelSize=STATIC_WHEEL_SIZE;
        this.timeWheel=new Object[wheelSize];
    }

    //自定义时间轮长度的构造
    public TimingWheel(ExecutorService executorService,int wheelSize){
        this(executorService);
        this.wheelSize = wheelSize;
        this.timeWheel = new Object[wheelSize];
    }






    //添加任务
    public int addTask(Task task){

        //获取任务延时时间
        int key = task.getKey();

        try{
            lock.lock();
            //取模计算插入位置   默认桶间隔为1
            int needBucketCount=key/1;
            int index = mod(needBucketCount, wheelSize);
            task.setIndex(index);
            //获取时间轮对应下标的的任务集合
            Set<Task> tasks = get(index);

            //判断是否为空
            if(tasks!=null){
                //获取周期数，也就是第几圈
                int cycleNum = cycleNum(needBucketCount, wheelSize);

                task.setCycleNum(cycleNum);
                //向set集中添加任务
                tasks.add(task);
            }else {
                //如果时间轮中对应的set集合不存在
                int cycleNum = cycleNum(needBucketCount, wheelSize);

                task.setCycleNum(cycleNum);

                //创建新的set集合
                Set<Task> newTaskSet = new HashSet<Task>();

                newTaskSet.add(task);

                //加入时间轮
                put(key,newTaskSet);

            }

            //任务id自增1 存入map中
            int incrementAndGet = taskId.incrementAndGet();
            taskMap.put(incrementAndGet,task);

            //任务数量总数加1
            taskCount.incrementAndGet();

        }finally {
            lock.unlock();
        }
        //启动时间轮
        start();
        //返回任务id
        return taskId.get();
    }


    //获取时间轮对应下标下的set任务集
    private Set<Task> get(int index){
        return (Set<Task>)timeWheel[index];
    }

    //把set集插入时间轮
    private void put(int key,Set<Task> task){
        //取模
        int wheelIndex = mod(key, wheelSize);
        //存入时间轮对应位置
        timeWheel[wheelIndex]=task;
    }

    //取模运算获取所在时间轮下标
    private int mod(int target,int mod){
        //目标时间加上当前时间轮指针时间
        target=target+tick.get();
        //位运算代替取模a & b=a%b  只能对2^n的数进行取模  target&(mod-1) 等价于 target%mod
        return target&(mod-1);
    }

    //获取任务所在时间轮周期
    private int cycleNum(int target,int wheelSize){
        return target/wheelSize;
    }






    //时间轮的启动与停止

    //启动
    public void start(){
        //保证只一次初始化成功
        if (!start.get()){
            //确保原子性更新状态值,与if内初始化操作保持原子性
            if (start.compareAndSet(false,true)){
                Thread thread = new Thread(new TriggerJob());
                thread.setName("start");
                thread.start();
            }
        }
    }

    //停止
    public void stop(Boolean b){

        if (b){
            //强制停止
            stop=true;
            executorService.shutdownNow();
        }else {
            if (taskCount.get()>0){
                //如果还有任务未执行完毕,阻塞主线程,直到任务执行完毕后被唤醒。
                try{
                    lock.lock();
                    condition.await();
                    stop=true;
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }finally {
                    lock.unlock();
                }

            }
            executorService.shutdown();
        }
    }

    //时间轮指针触发任务
    private class TriggerJob implements Runnable{


        @Override
        public void run() {
            int index=0;
            while (!stop){
                try {
                    //取出此时index的任务 并从时间轮移除。
                    Set<Task> tasks = remove(index);


                    //真正执行定时任务
                    for (Task task : tasks) {
                        executorService.submit(task);
                    }

                    //判断下一个指针是否指向时间轮当前周期最后一个节点
                    if(++index>wheelSize-1){
                        //初始化指针
                        index=0;
                    }

                    //指针刻度增1
                    tick.incrementAndGet();

                    TimeUnit.SECONDS.sleep(1);
                    System.out.println("推进1ms");

                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    //取出index的对应任务,并从时间轮移除。
    private Set<Task> remove(int key){
        //临时存储不是此周期内的任务，用来更新时间轮任务
        HashSet<Task> tempTasksSet = new HashSet<>();
        //取出的任务数据
        HashSet<Task> resultTaskSet = new HashSet<>();


        Set<Task> tasksSet = (Set<Task>)timeWheel[key];

        if (tasksSet== null){
            return resultTaskSet;
        }

        for (Task task : tasksSet) {
            //判断是否是当前周期的任务
            if (task.getCycleNum()== 0){
                //取出任务
                resultTaskSet.add(task);
                //唤醒主线程,用于停止任务时,主线程等待所有延时任务执行完毕。被唤醒。
                size2Notify();

            }else {
                //不属于当前周期的任务，周期减一，继续存入时间轮
                task.setCycleNum(task.getCycleNum()-1);
                tempTasksSet.add(task);
            }

            //把不属于当前周期的任务存入时间轮
            timeWheel[key]=tempTasksSet;

        }

        return resultTaskSet;

    }

    //唤醒方法
    private void size2Notify(){
        try {
            lock.lock();
            //任务量减一
            int count = taskCount.decrementAndGet();
            if (count== 0){
                //时间轮所有任务执行完毕
                //唤醒
                condition.signal();
            }
        }finally {
            lock.lock();
        }

    }









}
