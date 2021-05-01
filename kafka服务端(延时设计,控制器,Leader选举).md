# Kafka服务端之延时设计，Crotroller与分区Leader选举

* [Kafka服务端之延时设计，Crotroller与分区Leader选举](#kafka服务端之延时设计crotroller与分区leader选举)
  * [服务端延时设计之时间轮](#服务端延时设计之时间轮)
      * [源码解释](#源码解释)
      * [时间轮的图片抽象](#时间轮的图片抽象)
      * [简易时间轮](#简易时间轮)
      * [时间轮处理逻辑:](#时间轮处理逻辑)
  * [服务端之延时操作](#服务端之延时操作)
      * [DelayedOperation](#delayedoperation)
      * [DelayedOperationPurgatory](#delayedoperationpurgatory)
      * [延时操作应用之延时生产](#延时操作应用之延时生产)
      * [延时操作应用之延时拉取](#延时操作应用之延时拉取)
  * [服务端之控制器](#服务端之控制器)
      * [Kafka在zookeeper](#kafka在zookeeper)
      * [Controller职责](#controller职责)
      * [Controller与其余Broker间通信](#controller与其余broker间通信)
      * [Controller的选举](#controller的选举)
  * [服务端之分区Leader选举](#服务端之分区leader选举)
      * [分区Leader源码选举原理](#分区leader源码选举原理)
      * [分区Leader选举策略](#分区leader选举策略)
      * [分区Leader选举时机](#分区leader选举时机)

## 服务端延时设计之时间轮

#### 源码解释

**源码上是这样解释时间轮的：**

> 一系列的定时任务循环列表是由一个简单的时间轮实现的。比如U为时间单位。长度为N的时间轮
>
> 即有N个存储桶。该时间轮可以在NU的时间间隔内存放定时任务。
>
> 每个桶包含属于相应时间范围内的计时器任务。在最初:
>
> 第一个桶存放[0,U)时间间隔内的Tasks,
>
> 第二个桶存放[U,2U)时间间隔内的Tasks,
>
> 第三个桶存放[2U,3U)时间间隔内的Tasks,
>
> ...
>
> 第N 个桶存放[U(N-1),UN)时间间隔内的Tasks.
>
> 在每个时间单位为U的的时间间隔内,(每个桶的时间范围中),计时器都会计时并移动到下一个桶,然
>
> 后使其中的计时任务到期。
>
> 因此计时器不会把当前时间的计时任务插入桶中,因为他已经过期。计时器会立刻触发过期任务,清
>
> 空的存储桶可用于下一回合。
>
> 假设，当前时间T的任务被触发执行后,该时间桶将成为[T+UN,T +(N + 1)U)存储桶。
>
> 时间轮的插入/删除(开始计时器/停止计时器)时间复杂度为O(1),而基于优先级的队列，如
>
> `java.util.concurrent.DelayQueue`和`java.util.Timer`。
>
> 时间轮有一个缺点,就是所存储的定时任务都在U*N的时间范围内,如果超出,就会溢出。
>
> 溢出的处理方案就是分级时间轮。一个层级时间轮,最低层的时间轮有最精细的分辨率,层级越高,
>
> 分辨率越粗糙。

#### 时间轮的图片抽象

我们用图片直观的描述时间轮即：

**1级时间轮：**

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Kafka一级时间轮.jpg)

**1-3级时间轮：**

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Kafka三级时间轮.jpg)

#### 简易时间轮

单纯的描述时间轮这种算法未免抽象，可以手动写一个简略的时间轮来了解：

首先，我们定义三个对象。

`Task.java` 定时任务的抽象方法。

`TaskEntry.java` 对Task进行封装，真正的定时任务实现逻辑。

`TimingWheel.java` 时间轮

> TimingWheel.java

首先，我们需要在时间轮中定义必要的字段：因为是简易时间轮，我们使用set来存储具体的定时任务。(Kafka中使用的是一个双向环形链表TimerTaskList)。时间轮是一个数组。demo为单层时间轮。

```java
//时间轮的默认长度,长度定义为了64，需为2的N次方，因为后面使用了位运算代替取模,且只支持2的N次方。
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
```

定义两个构造:

```java
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
```

我们需要把定时任务放入此时间轮，在放入之前，我们还需要一些前戏。比如计算当前任务在时间轮中桶下标`mod()`,计算任务所处时间轮周期数`cycleNum()`等方法。

我们需要知道当前的定时任务要存放在时间轮中的哪个桶中，由于我们定义的时间轮桶间隔为1，即，我们需要通过定时任务的过期时间int:target，和时间轮总长度int:mod取模即为桶的index。

```java
//取模运算获取所在时间轮下标
private int mod(int target,int mod){
    //目标时间加上当前时间轮指针时间
    target=target+tick.get();
    //位运算代替取模a & b=a%b  只能对2^n的数进行取模  target&(mod-1) 等价于 target%mod
    return target&(mod-1);
}
```

不仅如此，我们实现的是单层时间轮，当时间轮总刻度不能满足定时任务时长时，而时间轮的桶又可以复用，我们便可以以周期的概念复用时间轮。

添加一个计算当前任务所在时间轮周期数的方法：

```java
//获取任务所在时间轮周期
private int cycleNum(int target,int wheelSize){
   return target/wheelSize;
}
```

知道了周期数和任务所在时间轮位置，便可以进行添加到Set定时任务集，并放入时间轮中:

```java
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
```

需要一个通过下标获取对应任务集的方法，即当前指针推进到某一时刻，取出对应时刻的延时任务。

```java
//获取时间轮对应下标下的set任务集
private Set<Task> get(int index){
   return (Set<Task>)timeWheel[index];
}
```

插入Set任务集的方法~

```java
//把set集插入时间轮
private void put(int key,Set<Task> task){
   //取模
   int wheelIndex = mod(key, wheelSize);
   //存入时间轮对应位置
   timeWheel[wheelIndex]=task;
}
```

我们可以自定义一个时间推进器，当时间推进到某一时刻，用来触发任务。

```java
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

                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }
    }
```

触发任务即把任务从时间轮中获取，如果是当前周期任务，取出，并把它从时间轮中删除掉。否则，周期数减一继续存入时间轮。

```java
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
```

有了这些方法，我们便可以启动时间轮：

```java
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
```

有启动就有停止，而停止有两种方式，一种强制停止，另一种阻塞主线程，等待所有未完成的任务完成后，唤醒主线程进行停止。

```java
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
```

停止方法:

```java
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
```

至此，时间轮的组建基本完毕，我们定义一个定时任务的抽象方法。添加我们必要的一些属性。

> Task.java

```java
public abstract class Task extends Thread {

    //延时时间
    private int key;
    //周期数
    private int cycleNum;
    //所在时间轮下标
    private int index;
    @Override
    public void run() {

    }
    public int getKey(){
        return key;
    }

    public void setKey(int key){
        this.key=key;
    }
    public int getCycleNum(){
        return cycleNum;
    }

    public void setCycleNum(int cycleNum){
        this.cycleNum=cycleNum;
    }
    public int getIndex(){
        return index;
    }

    public void setIndex(int cycleNum){
        this.index=index;
    }
}
```

此时，我们可以把时间轮的组件进行组装，然后试运行。

首先，我们通过继承Task抽象类封装一个真正的定时任务。

> TaskEntry.java

```java
public  class TaskEntry extends Task {
    private String msg;

    public TaskEntry(String msg){
        this.msg=msg;
    }


    @Override
    public void run() {
        System.out.println(msg);
    }

}

```

然后，便可组装时间轮，存入定时任务。

```java
    public static void main(String[] args) {
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        TimingWheel wheel = new TimingWheel(threadPool);


        TaskEntry entry = new TaskEntry("任务执行了");
        entry.setKey(10);
        wheel.addTask(entry);
        wheel.start();

    }
```

当然，写在Main函数里实则有些简陋，在Web项目中，我们可以把时间轮注入在spring容器内。启动的方法也只需调用一次即可。

#### 时间轮处理逻辑:

假设一个时间轮tickMs为1ms,且wheelSize等于20。那么，该时间轮的总时间跨度为1*20，20ms。初始情况下当

前时间为currentTime(表盘指针)位置为0,此时有一个2ms的定时任务。由于桶内时间间隔为左闭右开区间,所以会

存放在第三个桶内,随着时间推移,2ms后,currentTime到达第三个桶,此时需要把第三个桶对应的TimeTaskList中的

任务进行相应的到期操作,并清空TimeTaskList,以供下一回合使用。此时,如果再次进来一个8ms的务,Timingwheel

会把它插入在第11个桶内。

> TimingWheel.scala源码:

```scala
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {
  private[this] val interval = tickMs * wheelSize
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  private[this] var currentTime = startMs - (startMs % tickMs) 
  @volatile private[this] var overflowWheel: TimingWheel = null
}
```

> 往时间轮中添加任务

```scala
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    //该任务的过期时间
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // 已经被取消
      false
    } else if (expiration < currentTime + tickMs) {
      // 已经过期
      false
    } else if (expiration < currentTime + interval) {
      // 存入相应的桶
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        queue.offer(bucket)
      }
      true
    } else {
      // 超出该时间轮的总间隔,添加到高一级的时间轮
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }
```

> 创建高一级时间轮

```scala
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          //高一级时间轮每个桶的跨度为次级时间轮的总跨度
          tickMs = interval,
          //高一级时间轮的桶个数与次级时间轮桶个数相同
          wheelSize = wheelSize,
          //高级时间轮的起始时间为次级时间轮的当前指针时间
          startMs = currentTime,
          //高级时间轮任务个数与次级时间轮一致  
          taskCounter = taskCounter,
          //高级时间轮存放任务的序列与次级同一个
          queue
        )
      }
    }
  }
```



**TimingWheel**:存储定时任务环形队列。底层数组实现。

**TimerTaskList**:TimingWheel数组中的每一项即TimerTaskList，TimerTaskList是一个双向环形链表。

**TimerTaskEntry**:TimerTaskList双向环形链表中每一项都是定时任务项TimerTaskEntry。

**TimerTask**:TimerTaskEntry中封装的真正的定时任务。

**tickMs**:时间轮中每个桶的基本时间跨度。

**wheelSize**:时间轮的桶的总个数。即时间轮总长度。

**interval**:时间轮的总跨度。通过公式计算得来**->tickMs*wheelSize**。

**currentTime**:表盘指针,时间轮当前所处时间。

**当时间轮进入一个大于当前时间轮总跨度的定时任务，Kafka提出了层级时间轮的概念。高级时间轮的桶个数与低级时间轮一致，高级时间轮的桶间隔为低级时间轮的总跨度。如：**

**1级时间轮**

桶个数(wheelSize)=20

桶间隔(tickMs)=1

时间轮总跨度(interval)=20

**2级时间轮**

桶个数(wheelSize)=20

桶间隔(tickMs)=20

时间轮总跨度(interval)=400

**2级时间轮**

桶个数(wheelSize)=20

桶间隔(tickMs)=400

时间轮总跨度(interval)=8000

**插入**

当currentTime指针当前时间为0时，插进来1个10Ms的定时任务，即插入1级时间轮的第十个桶，此时，又进来一个350m的定时任务，1级时间轮总跨度无法满足，即创建2级时间轮，2级时间轮每个桶间隔为20，遂，插入第18个桶，当此时进来的任务为500时，2级时间轮的总时间跨度也无法满足，以此类推，创建3级时间轮，插入3级时间轮第2个桶。

**执行**

随着时间过去，以450m，三级时间轮为例，此时任务处在三级时间轮的第2个桶内，当currenTime指向该桶时，在[400,800)范围内的TimeTaskList中的任务会被提交到二级时间轮，如定时为500ms的在三级时间轮到期时，剩余50ms提交二级时间轮的第3个桶内，二级时间轮该桶过期时，剩余的10ms会被提交到1级时间轮中，10ms后，一级时间轮该桶到期，触发任务执行。

**kafka的时间轮，时间推进又是怎么做的呢？**

kafka借助了JDK中DelayQueue来协助推进，kafka会把存有定时任务的TimerTaskList都加入到DelayQueue中，根据TimerTaskList对应的超时时间expiration来排序，最短的超时时间排在队头，kafka通过一个名为ExpiredOperationReaper的线程来获取DelayQueue中已经超时的任务列表。然后，根据过期时间expiration进行推进时间轮，获取到任务之后，对任务进行操作，直接执行，或者降级时间轮。

**那么问题来了，既然定时任务最终还是存放在JDK中的DelayQueue队列中，那为何还需要时间轮的算法。kafka此举是否是脱裤子放屁的行为？**

答案是否定的。DelayQueue插入和删除的平均时间复杂度为O(nlogn),无法满足高性能的kafka，而时间轮算法，使用TimerTaskList双向环形链表，可以把插入删除的时间复杂度降为O(1)，属于将定时任务先进行分组，然后进行插入DelayQueue，大大提高了性能。而最终使用DelayQueue来存储，并根据过期时间来推进时间轮。是因为，当我们取出对应任务时，我们便可以精准一次推进对应的世界轮指针。比如，如果我们使用每秒定时推进一次，那么一个100ms的任务，我们要无理由的浪费前99次的推进资源，而kafka这种方式，便可一次精准推进到100的时间刻度。不得不说，DelayQueue与时间轮的结合堪称完美操作。

## 服务端之延时操作

kafka所有的延时任务底层都是通过时间轮实现的，对于延时操作，kafka对延时操作进行了封装DelayedOperation，还提供了一个名为DelayedOperationPurgatory的类对延时操作DelayedOperation进行管理。DelayedOperationPurgatory字面意思为延时操作炼狱，延时操作的添加，便进入到DelayedOperationPurgatory进行洗涤，等待延时任务过期，触发，升入天堂。

我们可以简单分析一下这两个组件：

#### DelayedOperation

DelayedOperation是一个封装了TimerTask的抽象类。其中参数：

```scala
//任务是否已经完成
private val completed = new AtomicBoolean(false)
//否应该尝试tryComplete()
private val tryCompletePending = new AtomicBoolean(false)
//线程锁
private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)
```

**方法**：

**def forceComplete(): Boolean = {}**

强制完成延时任务的操作(如果尚未完成)，内部调用onComplete()完成任务，以下情况可触发此函数：

1.该任务在tryComplete()方法中被验证可完成

2.任务到期，必须立即执行

此外,该方法提供原子性操作，即并发线程同时完成此任务，只有第一个线程成功完成并返回true，其余返回false.

```scala
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // 从时间轮中取消此定时任务
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }
```

**def isCompleted: Boolean = completed.get()**

校验延时任务是否已经完成。

**def onExpiration(): Unit**

抽象方法，当延时任务因为操作时间过期而被强制执行时，将回调此函数，由子类进行实现任务超时的具体逻辑。

**def onComplete(): Unit**

抽象方法，延时任务的具体业务逻辑，由子类进行实现。并且在forceComplete()中精准调用一次。

**def tryComplete(): Boolean**

试图执行延时任务。检测执行条件是否满足，满足则调用forcecomplete()进行执行。

**private[server] def maybeTryComplete(): Boolean = {}**

线程安全版的tryComplete()，拿到锁的线程进入if,如果没有其他线程运行maybeTryComplete()方法。则retry为刚刚tryCompletePending.set(false)设置的fasle。任务未完成不再重试。如果有其他线程,则不会拿到锁,进入else分支，调用!tryCompletePending.getAndSet(true)方法,设置retry为true。可以进行重试完成延时操作。
可以理解为,当多个线程进入此方法时,只有一个线程拿到锁，进行tryComplete()尝试检测任务是否可以完成。如果不满足完成条件,而其他线程会给拿到锁的线程增加重试的机会。

```scala
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false //是否重试
    var done = false //操作是否已经完成
    do {
      if (lock.tryLock()) {
        try {
          tryCompletePending.set(false)
          done = tryComplete()
        } finally {
          lock.unlock()
        }
        retry = tryCompletePending.get()
      } else {
        retry = !tryCompletePending.getAndSet(true)
      }
    } while (!isCompleted && retry)
    done
  }
```

**override def run(): Unit = {}**

延时操作过期后，会提交到systemTimer.taskExecutor线程执行,其中会调用forceComplete()方法强制执行延时操作,后调用onExpiration执行延时任务到期的操作。
即，组合调用forceComplete()和onExpiration()

#### DelayedOperationPurgatory

DelayedOperationPurgatory类对延时操作的抽象类DelayedOperation进行管理。延时操作被创建后会被放入DelayedOperationPurgatory中，Purgatory一词译为 ’ 炼狱 ‘，此处有延时任务在炼狱中洗涤之意。

> DelayedOprationPurgatory的半生对象

```scala
object DelayedOperationPurgatory {

  private val Shards = 512 // 对key的监控列表，也就是Operations进行切分,减少锁的争用。

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}
```

> DelayedOperationPurgatory类

```scala
/**
 * 记录某个延时时间所对应的延时操作，并进行过期操作。
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,//炼狱名称
                                                             //定时器，即时间轮
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             //控制删除线程移除时间轮Bucket桶内过期延时请求的频率，默认为1ms一次。
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {}
```

在DelayedOperationPurgatory中的结构大致为：

- **class Watchers** 

  封装了key与延时操作列表的对应关系。其中延时操作底层使用ConcurrentLinkedQueue存储

- **class WatcherList**

  key与Watchers的映射关系，底层使用ConcurrentHashMap实现。

- **def tryCompleteElseWatch**

  DelayedOperationPurgatory的核心方法，校验延时任务是否可完成，并尝试完成延时操作。

- **def checkAndComplete**

  DelayedOperationPurgatory的核心方法，校验某个key对应的延时操作是否可以完成，如果可以，则完成。

- **class ExpiredOperationReaper**

  过期操作收割者线程。

- **def advanceClock**

  1.推进时间轮 2.定期清理watchersByKey中已经完成的定时任务。

> class Watchers

```scala
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // 当前Watchers中的延时操作数量，这里时间复杂度为O(n),所以尽可能使用isEmpty().
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T) {}
  	
    // 遍历列表，并尝试完成其中的定时任务。
    def tryCompleteWatched(): Int = {}
    //取消操作
    def cancel(): List[T] = {}
    // 遍历列表，清除已经被其他线程完成的操作。
    def purgeCompleted(): Int = {
      
  }
```

> class WatcherList

```scala
 /**
   * 与Key与Watchers的映射。底层使用CurrentHashMap实现。
   */
private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * 返回所有的Watchers，返回的Watchers可能会被其他线程删除
     */
    def allWatchers = {
      watchersByKey.values
    }
}
```

> def tryCompleteElseWatch

此方法为DelayedOperationPurgatory的核心方法之一，用于检测该定时任务是否可以完成，并尝试完成。

- 一个延时操作可能被多个key监控，即一个延时操作可能会存在于多个Watchers中。
- 对于某些延时操作，可能在加入部分Watchers中时就被完成，对于这些操作，会被认为已经完成，且不会添加到剩余的Watchers中。
- 过期收割者线程(expirationReaper)会从存在该操作的所有Watchers 列表中删除该操作。

```scala
   /**
   * @param operation 需要被检测是否可完成的延时操作
   * @param watchKeys 此延时任务所关联的所有keys
   * @return 返回是否是此方法调用者所完成的延时任务
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // tryComplete()方法的开销一般与keys的数量成正比。
    // 如果有很多key,为每个key调用tryComplete()方法的成本会很大。
    // 如果，我们用下面的逻辑来处理，即，调用tryComplete()方法，
    // 如果操作没有完成，我们只需要把操作添加到所有Key对应的Watchers中，
    // 然后再次调用tryComplete()。
    // 此时，如果还未完成，我们可以把他放入所有的key关联的Watchers中。
    // 这样，此延时操作就不会错过未来的任何触发操作。
    // 但是，也有一个弊端，就是如果此线程检测未完成，但延时任务被其他线程完成，
    // 而此线程又将该操作添加到了所有key关联的Watchers中。
    // 此情况是小问题,无需顾虑,因为expirationReaper线程会定期清理掉它。

    // 此时，第一次尝试,还未加入所有的Watchers中,能尝试完成此操作的只有当前线程,
    // 所以线程是安全的,只需调用trycomplete()。--第一次调用tryComplete()
    var isCompletedByMe = operation.tryComplete()
    //如果任务已经完成，返回true即可。
    if (isCompletedByMe)
      return true


    //如果未完成


    var watchCreated = false

    //将所有的key进行遍历,如果watcherKeys中遍历所有的key都未完成。则加入所有key对应的Watchers。
    for(key <- watchKeys) {
      // 如果操作已经完成，说明是被其他线程完成，则停止向剩余的Watchers列表中添加此延时任务。并返回false
      if (operation.isCompleted)
        return false

      //如果未完成,则加入到当前key对应的Watchers。
      watchForOperation(key, operation)

      //炼狱中的延时任务数增1
      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }


    //再次调用tryComplete()由于此时延时任务可能存在于watchers中，即可能被其他线程所完成。
    //要调用maybeTryComplete()来保证线程安全的执行延时操作 --第二次调用tryComplete()
    isCompletedByMe = operation.maybeTryComplete()
    //已完成，则是线程安全方式下的完成，即还是当前线程完成的。遂返会true.
    if (isCompletedByMe)
      return true

    //如果还未完成,将其加入到时间轮中。
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      //再次检测是否完成,已完成则从时间轮中取消
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }
```

> def checkAndComplete

校验某个key对应的延时操作是否可以完成，如果可以，则完成。 

```scala
  /**
   * @return 此次完成的延时操作数量
   */
  def checkAndComplete(key: Any): Int = {
    //先从WatcherLists中取出对应的WatcherList
    val wl = watcherList(key)
    //watchersList中对应的Watchers
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    //试图完成其中的操作 并返回完成数量
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }
```

#### 延时操作应用之延时生产

kafka生产者客户端发送消息时，如果acks的参数为-1，那么就意味消息到达kafka之后，要等ISR中所有副本都拉取到消息后，才能收到kafka服务端的响应。

**DelayedProduce延时生产**

在服务端收到KafkaProduce的请求时:

> kafkaApis.scala

```scala
case ApiKeys.PRODUCE => handleProduceRequest(request)
```

kafka服务端会调用`replicaManager.appendRecords()`，在appendRecords()方法中，会进行创建DelayedProduce:

```scala
val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
```

然后将delayedProduce放入delayedProducePurgatory中。

```scala
delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
```

DelayedProduce继承了DelayedOperation。首先，进行每个分区的acksPending标记初始化。

```scala
produceMetadata.produceStatus.foreach {
    case (topicPartition, status) =>
    //如果没有设置Error
    if (status.responseStatus.error == Errors.NONE) {
      status.acksPending = true
      // 预设置ERROR属性,超时时间内ISR副本完成同步,则清除此状态。
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      status.acksPending = false
}
```

DelayedProduce实现了DelayedOperation中的tryComplete()方法，此方法校验了是否满足一定条件。满足执行条件时，调用forceComplete()强制完成延时任务。执行条件为：

- 此Broker不再是Leader。设置Error，并完成延时任务。
- ISR中所有副本完成了同步。
- 副本同步中出现了Error。

```scala
  override def tryComplete(): Boolean = {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // 跳过那些已经被满足的分区
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>
            if (partition eq ReplicaManager.OfflinePartition)
              (false, Errors.KAFKA_STORAGE_ERROR)
            else
              partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            //此broker不再是leader
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // 响应中出现error||此分区leader副本的HW大于requiredOffse，所有副本都同步完成
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }
    //检测全部分区的acksPending标记是否为false,
    //即全部分区是否满足A所在broker不再是leader或者B分区leader副本的HW大于requiredOffse||响应中出现Error
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      //强制完成延时任务，即返回响应到客户端
      forceComplete()
    else
      false
  }
```

在tryComplete()方法中，当所有分区满足条件之后，调用了forceComplete()。如前文所述，在forceComplete()中会精准调用一次onComplete()，此方法在DelayedProduce中的实现为：

```scala
override def onComplete() {
    //为每个分区配置响应状态
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    //响应回调,返回响应状态。
    responseCallback(responseStatus)
  }
```

**延时生产总体流程图**：

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\DelayedProduce延时生产流程图.png)

#### 延时操作应用之延时拉取

当follower副本向服务端发起请求，此时follower已经同步到了Leader的最新消息位置，重复的空拉取浪费资

源，所以，kafka在拉取不到消息最小值fetch.min.bytes的时候，会进行创建延时拉取任务。延时拉取的流程与延

时生产基本一致，延时拉取分为副本同步Leader的拉取，和消费者的拉取。kafka服务端在收到延时拉取的请求

时。拉取消息小于fetch.min.bytes，便会创建一个DelayedFetch的延时任务，放入DelayedOperationPurgatory

中。

在DelayedFetch中同样继承了DelayedOperation，并实现了def tryComplete()方法。当满足下列条件之一时，调

用forceComplete()方法。

1. 此Broker不再是它试图拉取某些分区的Leader Broker。
2. 此Broker找不到它试图拉取的某些分区信息。
3. 拉取的偏移量不在日志的最后一个分段。
4. 从分区中拉取的消息超过了最小字节。
5. 分区位于此代理的离线日志目录中。
6. Broker虽是Leader,但必需的epoch被隔离了。

> DelayedFetch.scala
>
> override def tryComplete()

```scala
  /**
   * 完成后，返回每个分区的有效数据
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    //遍历每个分区的数据
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        //获取拉取消息的起始偏移量
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val partition = replicaManager.getPartitionOrException(topicPartition,
              expectLeader = fetchMetadata.fetchOnlyLeader)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)
            //获取日志偏移量，其中包括HW位置，LOG的最后一条，
            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            //上一次拉取的偏移量是否发生变化,如果发生变化,则说明可能已有消息写入。有机会拉取到数据
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                //endOffset相较于fetchOffset位于更old的日志分段中，
                //可能刚刚Leader宕机更换了新的Leader发生了日志截断。满足条件3.因为当前活跃的日志分段还是endOffset所在的日志分段。
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                //要拉取的位移不在activSegment中。但是endOffset却在activeSegment中，可能当前分区刚刚生成了新的activeSegment就会发生此情况。
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                //要拉取的位移和最后一条消息位置在同一个日志分段中，此时对比两个位移，如果要拉取位移小于endOffset。则累计accumulatedSize字节数。
                //跳过限流的副本
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }
          }
        } catch {
          case _: KafkaStorageException => // 满足5 日志处于离线日志目录中，即分区处于不可用状态。
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // 满足2 Broker找不到需要拉取分区的信息。
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException =>
            // 满足6 为防止消息丢失，kafka引入消息版本即Leader Epoch,
            // 也就是虽然Broker是分区的Leader Broker，但请求的版本不是分区的最新版本号，则满足条件，立即完成延时拉取。
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // 满足1,此Broker不再是Leader。
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // 满足4  所拉取字节数大于最小字节数。
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
       forceComplete()
    else
      false
  }
```

> override def tryComplete()

```scala
 /**
   * Upon completion, read whatever data is available and pass to the complete callback
   * 读取任何有效的数据进行回调。
   */
  override def onComplete() {
    //再次读取数据
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      fetchIsolation = fetchMetadata.fetchIsolation,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      quota = quota)
    //把数据与分区对应进行封装。
    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
        result.lastStableOffset, result.info.abortedTransactions)
    }

    //回调响应给客户端
    responseCallback(fetchPartitionData)
  }
```

以上，即为Kafka服务端的延时生产，延时拉取的大致流程，在Kafka服务端，除了延时生产和延时拉取，还有

- DelayedHeartbeat：心跳延迟操作组件。
- DelayedJoin：再均衡时，为了等待当前消费者组内消费者加入消费者组。
- DelayedCreateTopics：等待主题的所有分区副本分配到Leader后调用回调函数返回客户端。
- DelayedTxnMarker：延时的事务状态变更添加到purgatory中，没有过期时间。例如，这些操作应该永远不会超时。
- DelayedDeleteRecords：延时删除
- ...

其底层实现逻辑都是时间轮。



## 服务端之控制器

kafka集群中存由一个或多个Broker组成，其中一个Broker会被选举为Controller，负责管理整个集群中所有分区

和副本的状态，比如，当某个分区Leader副本出现故障时，由Controller为该分区选举新的Leader副本，当检测

某个分区的ISR集合发生变化时，控制器负责通知所有Broker更新元数据，再或者，当使用Kafka-topic.sh脚本为

某个topic增加分区数量时，还是Controller负责分区的重新分配。

在kafka早期的版本中，对于分区和副本的管理依赖于zookeeper,每一个Broker都会在zookeeper上注册

Watcher(监视器)，以至于zookeeper中出现大量Watcher，容易出现脑裂和羊群效应，以及zookeeper集群过

载。

**脑裂效应：**

由于网络通信问题，导致一个集群被物理分为两个或多个独立运行的小集群，每个小集群都会有自己的Leader对外提供服务，当网络恢复后，小集群合并为一个集群，就会出现多个活动的Leader节点。

缺点：破坏集群和对外服务的一致性。

解决办法：添加冗余的心跳线,尽量减少“脑裂”发生机会，启用磁盘锁，在发生脑裂的时候可以协调控制对资源的访问，设置仲裁机制。

**羊群效应：**

由于某一节点A被大量客户端进行监控时，当A节点发生变化时候只对某一个客户端有影响，但由于所有节点都对A节点进行了监控，对于其他没有影响的客户端也会受到通知，这种不必要的通知即羊群效应。

新的kafka版本中，只有Kafka Controller在Zookeeper上注册相应的监控器，其他的Broker极少的需要监听Zookeeper中的数据变化，不过每个Broker还是对/Controller节点设置监听器，以此监听节点的数据变化。



#### Kafka在zookeeper

通过shell命令查看Kafka在Zookeeper的目录：

```shell
[zk: localhost:2181(CONNECTED) 9] ls /
[cluster, controller_epoch, controller, brokers, zookeeper, admin, isr_change_notification, consumers, log_dir_event_notification, latest_producer_id_block, config]
```

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/kafka在zk.png)

1. 记录了Topic所有分区及AR集合
2. 记录了某分区的Leader副本所在的BrokerId，Leadr_epoch,ISR集合，zk版本信息。
3. 记录了需要进行副本重分配的分区。
4. 记录了需要进行优先副本选举的分区，优先副本在创建分区的第一个副本。
5. 记录了删除的Topic。
6. 记录了当前Controller的年代版本信息。
7. 记录了当前Controller的id。
8. 记录了一段时间内ISR的列表变化的分区信息。
9. 记录了一些配置信息。

#### Controller职责

**监听分区变化**

- /admin/reassign_partitions节点注册PartitionReassignmentHandler(处理分区重分配)
- /isr_change_notification节点注册IsrChangeNotificetionHandler(处理ISR集合变更动作)
- /admin/preferred-replica-election节点添加PreferredReplicaElectionHandler(处理优先副本选举动作)

**监听Topic变化**

- /brokers/topics节点添加TopicChangeHandle(处理topic增减动作)
- /admin/delete_topics节点添加TopicDeletionHandler(处理删除主题的动作)

**监听Broker变化**

- /Broker/ids添加BrokerChangeHandler(处理broker增减的变化)

**更新集群元数据信息**

**启动并管理分区状态机和副本状态机**

**从Zookeeper中获取所有Topic，分区，broker相关信息进行管理，并对所有topic对应的/brokers/topics/&lt;topic>节点添加PartitionModificationsHandle监听topic的分区分配变化。**

**如果auto.leader.rebalance.enable 设置为 true，则还会开启一个名为“auto-leader-rebalance-task”的定时任务来负责维护分区的优先副本的均衡**

#### Controller与其余Broker间通信

kafka维护了一个brokerStateInfo(HashMap)，key为brokerId , Value为ControllerBrokerStateInfo，Controller为每个Broker维护了一个LinkedBlockingQueue队列，LinkedBlockingQueue是一个阻塞队列，内部由两个ReentrantLock来保障出入队列的线程安全。且添加删除操作不互斥，可同时进行，增加吞吐。ControllerEventManager通过ControllerEventThread(`val ControllerEventThreadName = "controller-event-thread`")线程来进行取出事件，进行处理。

#### Controller的选举

**时机**

- /Controller节点被手动删除，触发选举。可手动删除，强制触发选举。
- /Controller节点被手动更新。

**选举**

Kafka控制器的选举较为简单，当触发选举时，每个Broker都会去尝试获取/Controller节点的BrokerId的值，如果没有获取到，则尝试创建/Controller，首个创建成功的Broker则成为Controller，每个Broker都会在内存中保存当前Controller的Id,即activeControllerId。Zookeeper中还会存在一个/controller_epoch节点，每次的控制器变更都会使此epoch增1，每次和Controller交互都会携带此字段，请求的epoch的值小于内存中的epoch值时，说明是过期请求，会被认定为无效请求，通过epoch来保证操作的唯一性。

```scala
  //选举方法，注册zk节点.注册成功即成为Controller
  private def elect(): Unit = {
    //去zk拿当前Controller的id,拿不到即为-1.
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    
    //如果拿到了ControllerId则说明已有Broker注册成功,直接返回.
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }
    
    //如果暂未Broker竞选成功.则试图竞选.
    try {
      //在zk上添加相关节点.
      val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
      //初始化版本信息
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = epochZkVersion
      activeControllerId = config.brokerId

      info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " +
        s"and epoch zk version is now ${controllerContext.epochZkVersion}")
      //进行上位操作,即成为Controller之前,完成一些操作.
      onControllerFailover()
    } catch {
      case e: ControllerMovedException =>
        maybeResign()

        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}", e)
        else
          warn("A controller has been elected but just resigned, this will result in another round of election", e)

      case t: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}. " +
          s"Trigger controller movement immediately", t)
        triggerControllerMove()
    }
  }
```



**登基**

竞选成功的Controller会回调此方法，完成以下操作。

- 初始化上下文信息，如当前所有的topic，所有的Broker，和所有的分区Leader信息。
- 启动控制器的通道管理器。
- 启动副本状态机。
- 启动分区状态机。

如果成为控制器的过程中发生异常，则会辞去当前的控制器，进行退位操作，确保新一轮的竞选。

```scala
 private def onControllerFailover() {
    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    //读取zk资源之前，注册监听器获取Broker/Topic的回调.
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)
    info("Initializing controller context")
    initializeControllerContext()
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    //初始化上下文初始化之后,状态机启动之前,发送UpdateMetadataRequest,接收活跃的Broker列表,
    //才可以调用,处理replicaStateMachine.startup()和partitionStateMachine.startup()一些初始化方法
    info("Sending update metadata request")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)

    replicaStateMachine.startup()
    partitionStateMachine.startup()

    //准备成为Controller
    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment(controllerContext.partitionsBeingReassigned.keySet)
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections, ZkTriggered)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = () => tokenManager.expireTokens,
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }
```



**退位**

当BrokerId与新的activeController值不一致时，需要退位，即关闭相关资源，注销监听器等。

```scala
 private def onControllerResignation() {
    debug("Resigning")
    // 注销监听器
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // 关闭Leader的再均衡调度
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0

    // 停止token过期校验调度
    if (tokenCleanScheduler.isStarted)
      tokenCleanScheduler.shutdown()

    //注销ISR集合监听
    unregisterPartitionReassignmentIsrChangeHandlers()
    // 关闭分区状态机
    partitionStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)
    // 关闭副本状态机
    replicaStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)

    controllerChannelManager.shutdown()
    controllerContext.resetContext()

    info("Resigned")
  }
```



## 服务端之分区Leader选举

#### 分区Leader源码选举原理

分区Leader副本的选举是由控制器负责具体实施，也就是控制器中的分区状态机PartitionStateMachine.scala来完成，在PartitionStateMachine.scala文件中，有一个`handleStateChanges()`方法，此方法封装了`doHandleStateChanges()`处理分区状态的合法变更，这包括：

- NonExistentPartition -> NewPartition 无效分区变更为新分区。=>从zk加载已分配副本到控制器缓存中。
- NewPartition -> OnlinePartition 新分区变更为在线分区。=>选举一个活跃的副本作为Leader，所有活跃副本均加入ISR，将Leader与ISR都写入zk。向每个活跃副本发送 LeaderAndIsr请求，向每个活跃的Broker发送UpdateMetadata请求。
- OnlinePartition ，OfflinePartition ->OnlinePartition =>为此分区的ISR集合和能获取LeaderAndIsr请求的副本集选举一个Leader.对于此分区，向每个活跃副本发送 LeaderAndIsr请求，向每个活跃的Broker发送UpdateMetadata请求。
- NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition=>分区状态设置为离线。
- OfflinePartition -> NonExistentPartition =>分区设置为无效分区。

> doHandleStateChanges()

doHandleStateChanges()方法以targetState为维度将调用进行区分，其中，当targetState为OnlinePartition，且分区为在线分区或离线分区时(OnlinePartition / OfflinePartition )会进行Leader选举操作。

```scala
val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
if (partitionsToElectLeader.nonEmpty) {
          //选举操作
          val (successfulElections, failedElections) = electLeaderForPartitions(partitionsToElectLeader, partitionLeaderElectionStrategyOpt.get)
          successfulElections.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
          failedElections
        } else {
          Map.empty
        }
       
```

#### 分区Leader选举策略

> doElectLeaderForPartitions

对于分区Leader的选举，kafka给出了四种选举策略。分别是：

```scala
sealed trait PartitionLeaderElectionStrategy
case object OfflinePartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
```

#### 分区Leader选举时机

选举策略由调用方传入，因选举时机不同而使用不同的策略。

- OfflinePartitionLeaderElectionStrategy

当创建分区(包括新建Topic时的创建分区)或分区上下线。使用此种策略，即按照AR集合中副本顺序查找第一个存活的副本，并且这个副本在ISR集合中。(因为AR集合顺序在分配时即被指定，不发生重分配，顺序不变，ISR集合副本顺序可能会发生改变)

对于此种策略，还有一个参数相关联。

```shell
#表示如果ISR集合中没有可用副本，是否从AR找第一个存活的副本作为Leader。
unclean.leader.election.enable=false(默认)
```

- ReassignPartitionLeaderElectionStrategy

当分区重分配时，也需要执行Leader选举动作，此时使用ReassignPartitionLeaderElectionStrategy策略，逻辑为，从重分配的AR列表中找到第一个存活的副本，且这副本在ISR中。

- PreferredReplicaPartitionLeaderElectionStrategy

发生优先副本选举时使用的策略，直接将优先副本设置为Leader,AR集合中第一个副本即为优先副本。

- ControlledShutdownPartitionLeaderElectionStrategy

某Broker节点优雅关闭，执行ControlledShutdown时，位于此节点的Leader均会下线，此时选举思路为，从AR中选择第一个存活的且位于ISR的副本，作为Leader，而且要确保这个副本不在正在关闭的节点上。







