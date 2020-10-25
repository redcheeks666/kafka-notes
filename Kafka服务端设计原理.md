# Kafka����˵����ԭ��

## �����֮ʱ����

#### Դ�����

**Դ��������������ʱ���ֵģ�**

> һϵ�еĶ�ʱ����ѭ���б�����һ���򵥵�ʱ����ʵ�ֵġ�����UΪʱ�䵥λ������ΪN��ʱ����
>
> ����N���洢Ͱ����ʱ���ֿ�����NU��ʱ�����ڴ�Ŷ�ʱ����
>
> ÿ��Ͱ����������Ӧʱ�䷶Χ�ڵļ�ʱ�����������:
>
> ��һ��Ͱ���[0,U)ʱ�����ڵ�Tasks,
>
> �ڶ���Ͱ���[U,2U)ʱ�����ڵ�Tasks,
>
> ������Ͱ���[2U,3U)ʱ�����ڵ�Tasks,
>
> ...
>
> ��N ��Ͱ���[U(N-1),UN)ʱ�����ڵ�Tasks.
>
> ��ÿ��ʱ�䵥λΪU�ĵ�ʱ������,(ÿ��Ͱ��ʱ�䷶Χ��),��ʱ�������ʱ���ƶ�����һ��Ͱ,Ȼ
>
> ��ʹ���еļ�ʱ�����ڡ�
>
> ��˼�ʱ������ѵ�ǰʱ��ļ�ʱ�������Ͱ��,��Ϊ���Ѿ����ڡ���ʱ�������̴�����������,��
>
> �յĴ洢Ͱ��������һ�غϡ�
>
> ���裬��ǰʱ��T�����񱻴���ִ�к�,��ʱ��Ͱ����Ϊ[T+UN,T +(N + 1)U)�洢Ͱ��
>
> ʱ���ֵĲ���/ɾ��(��ʼ��ʱ��/ֹͣ��ʱ��)ʱ�临�Ӷ�ΪO(1),���������ȼ��Ķ��У���
>
> `java.util.concurrent.DelayQueue`��`java.util.Timer`��
>
> ʱ������һ��ȱ��,�������洢�Ķ�ʱ������U*N��ʱ�䷶Χ��,�������,�ͻ������
>
> ����Ĵ��������Ƿּ�ʱ���֡�һ���㼶ʱ����,��Ͳ��ʱ�������ϸ�ķֱ���,�㼶Խ��,
>
> �ֱ���Խ�ֲڡ�

#### ʱ���ֵ�ͼƬ����

������ͼƬֱ�۵�����ʱ���ּ���

**1��ʱ���֣�**

![](C:\Users\EDZ\Desktop\�½��ļ���\picture\Kafkaһ��ʱ����.jpg)

**1-3��ʱ���֣�**

![](C:\Users\EDZ\Desktop\�½��ļ���\picture\Kafka����ʱ����.jpg)

#### ����ʱ����

����������ʱ���������㷨δ����󣬿����ֶ�дһ�����Ե�ʱ�������˽⣺

���ȣ����Ƕ�����������

`Task.java` ��ʱ����ĳ��󷽷���

`TaskEntry.java` ��Task���з�װ�������Ķ�ʱ����ʵ���߼���

`TimingWheel.java` ʱ����

> TimingWheel.java

���ȣ�������Ҫ��ʱ�����ж����Ҫ���ֶΣ���Ϊ�Ǽ���ʱ���֣�����ʹ��set���洢����Ķ�ʱ����(Kafka��ʹ�õ���һ��˫��������TimerTaskList)��ʱ������һ�����顣demoΪ����ʱ���֡�

```java
//ʱ���ֵ�Ĭ�ϳ���,���ȶ���Ϊ��64����Ϊ2��N�η�����Ϊ����ʹ����λ�������ȡģ,��ֻ֧��2��N�η���
private static final int STATIC_WHEEL_SIZE=64;
//������Ϊʱ����
private Object[] timeWheel;
private int wheelSize;
//�̳߳�
private ExecutorService executorService;
//ʱ��������������
//AtomicBiInteger�ṩԭ�Ӳ���
private AtomicInteger taskCount=new AtomicInteger();
//ʱ����ֹͣ��ʶ
private volatile boolean stop=false;
//������, ����stop
private Lock lock=new ReentrantLock();
private Condition condition = lock.newCondition();
//ʹ��ԭ����,��ʼ��ֻ��Ҫһ���̣߳�ȷ��ֻһ�γ�ʼ������
private volatile AtomicBoolean start=new AtomicBoolean(false);
//ָ��
private AtomicInteger tick=new AtomicInteger();
//����id
private AtomicInteger taskId=new AtomicInteger();
//��id��������
private Map<Integer, Task> taskMap =new HashMap<>();
```

������������:

```java
//Ĭ�Ϲ���
public TimingWheel(ExecutorService executorService){
    this.executorService=executorService;
    this.wheelSize=STATIC_WHEEL_SIZE;
    this.timeWheel=new Object[wheelSize];
}
//�Զ���ʱ���ֳ��ȵĹ���
public TimingWheel(ExecutorService executorService,int wheelSize){
    this(executorService);
    this.wheelSize = wheelSize;
    this.timeWheel = new Object[wheelSize];
}
```

������Ҫ�Ѷ�ʱ��������ʱ���֣��ڷ���֮ǰ�����ǻ���ҪһЩǰϷ��������㵱ǰ������ʱ������Ͱ�±�`mod()`,������������ʱ����������`cycleNum()`�ȷ�����

������Ҫ֪����ǰ�Ķ�ʱ����Ҫ�����ʱ�����е��ĸ�Ͱ�У��������Ƕ����ʱ����Ͱ���Ϊ1������������Ҫͨ����ʱ����Ĺ���ʱ��int:target����ʱ�����ܳ���int:modȡģ��ΪͰ��index��

```java
//ȡģ�����ȡ����ʱ�����±�
private int mod(int target,int mod){
    //Ŀ��ʱ����ϵ�ǰʱ����ָ��ʱ��
    target=target+tick.get();
    //λ�������ȡģa & b=a%b  ֻ�ܶ�2^n��������ȡģ  target&(mod-1) �ȼ��� target%mod
    return target&(mod-1);
}
```

������ˣ�����ʵ�ֵ��ǵ���ʱ���֣���ʱ�����̶ܿȲ������㶨ʱ����ʱ��ʱ����ʱ���ֵ�Ͱ�ֿ��Ը��ã����Ǳ���������ڵĸ����ʱ���֡�

���һ�����㵱ǰ��������ʱ�����������ķ�����

```java
//��ȡ��������ʱ��������
private int cycleNum(int target,int wheelSize){
   return target/wheelSize;
}
```

֪��������������������ʱ����λ�ã�����Խ�����ӵ�Set��ʱ���񼯣�������ʱ������:

```java
    //�������
    public int addTask(Task task){

        //��ȡ������ʱʱ��
        int key = task.getKey();

        try{
            lock.lock();
            //ȡģ�������λ��   Ĭ��Ͱ���Ϊ1
            int needBucketCount=key/1;
            int index = mod(needBucketCount, wheelSize);
            task.setIndex(index);
            //��ȡʱ���ֶ�Ӧ�±�ĵ����񼯺�
            Set<Task> tasks = get(index);

            //�ж��Ƿ�Ϊ��
            if(tasks!=null){
                //��ȡ��������Ҳ���ǵڼ�Ȧ
                int cycleNum = cycleNum(needBucketCount, wheelSize);

                task.setCycleNum(cycleNum);
                //��set�����������
                tasks.add(task);
            }else {
                //���ʱ�����ж�Ӧ��set���ϲ�����
                int cycleNum = cycleNum(needBucketCount, wheelSize);

                task.setCycleNum(cycleNum);

                //�����µ�set����
                Set<Task> newTaskSet = new HashSet<Task>();

                newTaskSet.add(task);

                //����ʱ����
                put(key,newTaskSet);

            }

            //����id����1 ����map��
            int incrementAndGet = taskId.incrementAndGet();
            taskMap.put(incrementAndGet,task);

            //��������������1
            taskCount.incrementAndGet();

        }finally {
            lock.unlock();
        }
        //����ʱ����
        start();
        //��������id
        return taskId.get();
    }
```

��Ҫһ��ͨ���±��ȡ��Ӧ���񼯵ķ���������ǰָ���ƽ���ĳһʱ�̣�ȡ����Ӧʱ�̵���ʱ����

```java
//��ȡʱ���ֶ�Ӧ�±��µ�set����
private Set<Task> get(int index){
   return (Set<Task>)timeWheel[index];
}
```

����Set���񼯵ķ���~

```java
//��set������ʱ����
private void put(int key,Set<Task> task){
   //ȡģ
   int wheelIndex = mod(key, wheelSize);
   //����ʱ���ֶ�Ӧλ��
   timeWheel[wheelIndex]=task;
}
```

���ǿ����Զ���һ��ʱ���ƽ�������ʱ���ƽ���ĳһʱ�̣�������������

```java
        @Override
        public void run() {
            int index=0;
            while (!stop){
                try {
                    //ȡ����ʱindex������ ����ʱ�����Ƴ���
                    Set<Task> tasks = remove(index);


                    //����ִ�ж�ʱ����
                    for (Task task : tasks) {
                        executorService.submit(task);
                    }

                    //�ж���һ��ָ���Ƿ�ָ��ʱ���ֵ�ǰ�������һ���ڵ�
                    if(++index>wheelSize-1){
                        //��ʼ��ָ��
                        index=0;
                    }

                    //ָ��̶���1
                    tick.incrementAndGet();
                    
                    TimeUnit.SECONDS.sleep(1);

                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }
    }
```

�������񼴰������ʱ�����л�ȡ������ǵ�ǰ��������ȡ������������ʱ������ɾ������������������һ��������ʱ���֡�

```java
    //ȡ��index�Ķ�Ӧ����,����ʱ�����Ƴ���
    private Set<Task> remove(int key){
        //��ʱ�洢���Ǵ������ڵ�������������ʱ��������
        HashSet<Task> tempTasksSet = new HashSet<>();
        //ȡ������������
        HashSet<Task> resultTaskSet = new HashSet<>();


        Set<Task> tasksSet = (Set<Task>)timeWheel[key];

        if (tasksSet== null){
            return resultTaskSet;
        }

        for (Task task : tasksSet) {
            //�ж��Ƿ��ǵ�ǰ���ڵ�����
            if (task.getCycleNum()== 0){
                //ȡ������
                resultTaskSet.add(task);
                //�������߳�,����ֹͣ����ʱ,���̵߳ȴ�������ʱ����ִ����ϡ������ѡ�
                size2Notify();

            }else {
                //�����ڵ�ǰ���ڵ��������ڼ�һ����������ʱ����
                task.setCycleNum(task.getCycleNum()-1);
                tempTasksSet.add(task);
            }

            //�Ѳ����ڵ�ǰ���ڵ��������ʱ����
            timeWheel[key]=tempTasksSet;

        }

        return resultTaskSet;

    }
```

������Щ���������Ǳ��������ʱ���֣�

```java
    //����
    public void start(){
        //��ֻ֤һ�γ�ʼ���ɹ�
        if (!start.get()){
            //ȷ��ԭ���Ը���״ֵ̬,��if�ڳ�ʼ����������ԭ����
            if (start.compareAndSet(false,true)){
                Thread thread = new Thread(new TriggerJob());
                thread.setName("start");
                thread.start();
            }
        }
    }
```

����������ֹͣ����ֹͣ�����ַ�ʽ��һ��ǿ��ֹͣ����һ���������̣߳��ȴ�����δ��ɵ�������ɺ󣬻������߳̽���ֹͣ��

```java
    //���ѷ���
    private void size2Notify(){
        try {
            lock.lock();
            //��������һ
            int count = taskCount.decrementAndGet();
            if (count== 0){
                //ʱ������������ִ�����
                //����
                condition.signal();
            }
        }finally {
            lock.lock();
        }

    }
```

ֹͣ����:

```java
    //ֹͣ
    public void stop(Boolean b){

        if (b){
            //ǿ��ֹͣ
            stop=true;
            executorService.shutdownNow();
        }else {
            if (taskCount.get()>0){
                //�����������δִ�����,�������߳�,ֱ������ִ����Ϻ󱻻��ѡ�
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

���ˣ�ʱ���ֵ��齨������ϣ����Ƕ���һ����ʱ����ĳ��󷽷���������Ǳ�Ҫ��һЩ���ԡ�

> Task.java

```java
public abstract class Task extends Thread {

    //��ʱʱ��
    private int key;
    //������
    private int cycleNum;
    //����ʱ�����±�
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

��ʱ�����ǿ��԰�ʱ���ֵ����������װ��Ȼ�������С�

���ȣ�����ͨ���̳�Task�������װһ�������Ķ�ʱ����

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

Ȼ�󣬱����װʱ���֣����붨ʱ����

```java
    public static void main(String[] args) {
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        TimingWheel wheel = new TimingWheel(threadPool);


        TaskEntry entry = new TaskEntry("����ִ����");
        entry.setKey(10);
        wheel.addTask(entry);
        wheel.start();

    }
```

��Ȼ��д��Main������ʵ����Щ��ª����Web��Ŀ�У����ǿ��԰�ʱ����ע����spring�����ڡ������ķ���Ҳֻ�����һ�μ��ɡ�

#### ʱ���ִ����߼�:

 		����һ��ʱ����tickMsΪ1ms,��wheelSize����20����ô����ʱ���ֵ���ʱ����Ϊ1*20��

20ms����ʼ�����,��ǰʱ��ΪcurrentTime(����ָ��)λ��Ϊ0,��ʱ��һ��2ms�Ķ�ʱ��������Ͱ

��ʱ����Ϊ����ҿ�����,���Ի����ڵ�����Ͱ��,����ʱ������,2ms��,currentTime���������

Ͱ,��ʱ��Ҫ�ѵ�����Ͱ��Ӧ��TimeTaskList�е����������Ӧ�ĵ��ڲ���,�����TimeTaskList,�Թ���

һ�غ�ʹ�á���ʱ,����ٴν���һ��8ms������,Timingwheel����������ڵ�11��Ͱ�ڡ�

> TimingWheel.scalaԴ��:

```scala
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {
  private[this] val interval = tickMs * wheelSize
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  private[this] var currentTime = startMs - (startMs % tickMs) 
  @volatile private[this] var overflowWheel: TimingWheel = null
}
```

> ��ʱ�������������

```scala
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    //������Ĺ���ʱ��
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // �Ѿ���ȡ��
      false
    } else if (expiration < currentTime + tickMs) {
      // �Ѿ�����
      false
    } else if (expiration < currentTime + interval) {
      // ������Ӧ��Ͱ
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        queue.offer(bucket)
      }
      true
    } else {
      // ������ʱ���ֵ��ܼ��,��ӵ���һ����ʱ����
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }
```

> ������һ��ʱ����

```scala
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          //��һ��ʱ����ÿ��Ͱ�Ŀ��Ϊ�μ�ʱ���ֵ��ܿ��
          tickMs = interval,
          //��һ��ʱ���ֵ�Ͱ������μ�ʱ����Ͱ������ͬ
          wheelSize = wheelSize,
          //�߼�ʱ���ֵ���ʼʱ��Ϊ�μ�ʱ���ֵĵ�ǰָ��ʱ��
          startMs = currentTime,
          //�߼�ʱ�������������μ�ʱ����һ��  
          taskCounter = taskCounter,
          //�߼�ʱ���ִ�������������μ�ͬһ��
          queue
        )
      }
    }
  }
```



**TimingWheel**:�洢��ʱ�����ζ��С��ײ�����ʵ�֡�

**TimerTaskList**:TimingWheel�����е�ÿһ�TimerTaskList��TimerTaskList��һ��˫��������

**TimerTaskEntry**:TimerTaskList˫����������ÿһ��Ƕ�ʱ������TimerTaskEntry��

**TimerTask**:TimerTaskEntry�з�װ�������Ķ�ʱ����

**tickMs**:ʱ������ÿ��Ͱ�Ļ���ʱ���ȡ�

**wheelSize**:ʱ���ֵ�Ͱ���ܸ�������ʱ�����ܳ��ȡ�

**interval**:ʱ���ֵ��ܿ�ȡ�ͨ����ʽ�������**->tickMs*wheelSize**��

**currentTime**:����ָ��,ʱ���ֵ�ǰ����ʱ�䡣

 		**��ʱ���ֽ���һ�����ڵ�ǰʱ�����ܿ�ȵĶ�ʱ����Kafka����˲㼶ʱ���ֵĸ���߼�ʱ���ֵ�Ͱ������ͼ�ʱ����һ�£��߼�ʱ���ֵ�Ͱ���Ϊ�ͼ�ʱ���ֵ��ܿ�ȡ��磺**

**1��ʱ����**

Ͱ����(wheelSize)=20

Ͱ���(tickMs)=1

ʱ�����ܿ��(interval)=20

**2��ʱ����**

Ͱ����(wheelSize)=20

Ͱ���(tickMs)=20

ʱ�����ܿ��(interval)=400

**2��ʱ����**

Ͱ����(wheelSize)=20

Ͱ���(tickMs)=400

ʱ�����ܿ��(interval)=8000

**����**

 		��currentTimeָ�뵱ǰʱ��Ϊ0ʱ�������1��10Ms�Ķ�ʱ���񣬼�����1��ʱ���ֵĵ�ʮ��Ͱ����ʱ���ֽ���һ��350m�Ķ�ʱ����1��ʱ�����ܿ���޷����㣬������2��ʱ���֣�2��ʱ����ÿ��Ͱ���Ϊ20���죬�����18��Ͱ������ʱ����������Ϊ500ʱ��2��ʱ���ֵ���ʱ����Ҳ�޷����㣬�Դ����ƣ�����3��ʱ���֣�����3��ʱ���ֵ�2��Ͱ��

**ִ��**

 		����ʱ���ȥ����450m������ʱ����Ϊ������ʱ����������ʱ���ֵĵ�2��Ͱ�ڣ���currenTimeָ���Ͱʱ����[400,800)��Χ�ڵ�TimeTaskList�е�����ᱻ�ύ������ʱ���֣��綨ʱΪ500ms��������ʱ���ֵ���ʱ��ʣ��50ms�ύ����ʱ���ֵĵ�3��Ͱ�ڣ�����ʱ���ָ�Ͱ����ʱ��ʣ���10ms�ᱻ�ύ��1��ʱ�����У�10ms��һ��ʱ���ָ�Ͱ���ڣ���������ִ�С�

**kafka��ʱ���֣�ʱ���ƽ�������ô�����أ�**

 		kafka������JDK��DelayQueue��Э���ƽ���kafka��Ѵ��ж�ʱ�����TimerTaskList�����뵽DelayQueue�У�����TimerTaskList��Ӧ�ĳ�ʱʱ��expiration��������̵ĳ�ʱʱ�����ڶ�ͷ��kafkaͨ��һ����ΪExpiredOperationReaper���߳�����ȡDelayQueue���Ѿ���ʱ�������б�Ȼ�󣬸��ݹ���ʱ��expiration�����ƽ�ʱ���֣���ȡ������֮�󣬶�������в�����ֱ��ִ�У����߽���ʱ���֡�

**��ô�������ˣ���Ȼ��ʱ�������ջ��Ǵ����JDK�е�DelayQueue�����У���Ϊ�λ���Ҫʱ���ֵ��㷨��kafka�˾��Ƿ����ѿ��ӷ�ƨ����Ϊ��**

 		���Ƿ񶨵ġ�DelayQueue�����ɾ����ƽ��ʱ�临�Ӷ�ΪO(nlogn),�޷���������ܵ�kafka����ʱ�����㷨��ʹ��TimerTaskList˫�����������԰Ѳ���ɾ����ʱ�临�ӶȽ�ΪO(1)�����ڽ���ʱ�����Ƚ��з��飬Ȼ����в���DelayQueue�������������ܡ�������ʹ��DelayQueue���洢�������ݹ���ʱ�����ƽ�ʱ���֡�����Ϊ��������ȡ����Ӧ����ʱ�����Ǳ���Ծ�׼һ���ƽ���Ӧ��������ָ�롣���磬�������ʹ��ÿ�붨ʱ�ƽ�һ�Σ���ôһ��100ms����������Ҫ�����ɵ��˷�ǰ99�ε��ƽ���Դ����kafka���ַ�ʽ�����һ�ξ�׼�ƽ���100��ʱ��̶ȡ����ò�˵��DelayQueue��ʱ���ֵĽ�Ͽ�������������

## �����֮��ʱ����

 		kafka���е���ʱ����ײ㶼��ͨ��ʱ����ʵ�ֵģ�������ʱ������kafka����ʱ���������˷�װ-DelayedOperation�����ṩ��һ����ΪDelayedOperationPurgatory�������ʱ����DelayedOperation���й���DelayedOperationPurgatory������˼Ϊ��ʱ������������ʱ��������ӣ�����뵽DelayedOperationPurgatory����ϴ�ӣ��ȴ���ʱ������ڣ��������������á�

���ǿ��Լ򵥷���һ�������������

#### DelayedOperation

DelayedOperation��һ����װ��TimerTask�ĳ����ࡣ���в�����

```scala
//�����Ƿ��Ѿ����
private val completed = new AtomicBoolean(false)
//��Ӧ�ó���tryComplete()
private val tryCompletePending = new AtomicBoolean(false)
//�߳���
private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)
```

**����**��

**def forceComplete(): Boolean = {}**

ǿ�������ʱ����Ĳ���(�����δ���)���ڲ�����onComplete()���������������ɴ����˺�����

1.��������tryComplete()�����б���֤�����

2.�����ڣ���������ִ��

����,�÷����ṩԭ���Բ������������߳�ͬʱ��ɴ�����ֻ�е�һ���̳߳ɹ���ɲ�����true�����෵��false.

```scala
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // ��ʱ������ȡ���˶�ʱ����
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }
```

**def isCompleted: Boolean = completed.get()**

У����ʱ�����Ƿ��Ѿ���ɡ�

**def onExpiration(): Unit**

���󷽷�������ʱ������Ϊ����ʱ����ڶ���ǿ��ִ��ʱ�����ص��˺��������������ʵ������ʱ�ľ����߼���

**def onComplete(): Unit**

���󷽷�����ʱ����ľ���ҵ���߼������������ʵ�֡�������forceComplete()�о�׼����һ�Ρ�

**def tryComplete(): Boolean**

��ͼִ����ʱ���񡣼��ִ�������Ƿ����㣬���������forcecomplete()����ִ�С�

**private[server] def maybeTryComplete(): Boolean = {}**

�̰߳�ȫ���tryComplete()���õ������߳̽���if,���û�������߳�����maybeTryComplete()��������retryΪ�ո�tryCompletePending.set(false)���õ�fasle������δ��ɲ������ԡ�����������߳�,�򲻻��õ���,����else��֧������!tryCompletePending.getAndSet(true)����,����retryΪtrue�����Խ������������ʱ������
�������Ϊ,������߳̽���˷���ʱ,ֻ��һ���߳��õ���������tryComplete()���Լ�������Ƿ������ɡ�����������������,�������̻߳���õ������߳��������ԵĻ��ᡣ

```scala
  private[server] def maybeTryComplete(): Boolean = {
    var retry = false //�Ƿ�����
    var done = false //�����Ƿ��Ѿ����
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

��ʱ�������ں󣬻��ύ��systemTimer.taskExecutor�߳�ִ��,���л����forceComplete()����ǿ��ִ����ʱ����,�����onExpirationִ����ʱ�����ڵĲ�����
������ϵ���forceComplete()��onExpiration()

#### DelayedOperationPurgatory

 		DelayedOperationPurgatory�����ʱ�����ĳ�����DelayedOperation���й�����ʱ������������ᱻ����DelayedOperationPurgatory�У�Purgatoryһ����Ϊ �� ���� �����˴�����ʱ������������ϴ��֮�⡣

> DelayedOprationPurgatory�İ�������

```scala
object DelayedOperationPurgatory {

  private val Shards = 512 // ��key�ļ���б�Ҳ����Operations�����з�,�����������á�

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

> DelayedOperationPurgatory��

```scala
/**
 * ��¼ĳ����ʱʱ������Ӧ����ʱ�����������й��ڲ�����
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,//��������
                                                             //��ʱ������ʱ����
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             //����ɾ���߳��Ƴ�ʱ����BucketͰ�ڹ�����ʱ�����Ƶ�ʣ�Ĭ��Ϊ1msһ�Ρ�
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {}
```

��DelayedOperationPurgatory�еĽṹ����Ϊ��

- **class Watchers** 

  ��װ��key����ʱ�����б�Ķ�Ӧ��ϵ��������ʱ�����ײ�ʹ��ConcurrentLinkedQueue�洢

- **class WatcherList**

  key��Watchers��ӳ���ϵ���ײ�ʹ��ConcurrentHashMapʵ�֡�

- **def tryCompleteElseWatch**

  DelayedOperationPurgatory�ĺ��ķ�����У����ʱ�����Ƿ����ɣ������������ʱ������

- **def checkAndComplete**

  DelayedOperationPurgatory�ĺ��ķ�����У��ĳ��key��Ӧ����ʱ�����Ƿ������ɣ�������ԣ�����ɡ�

- **class ExpiredOperationReaper**

  ���ڲ����ո����̡߳�

- **def advanceClock**

  1.�ƽ�ʱ���� 2.��������watchersByKey���Ѿ���ɵĶ�ʱ����

> class Watchers

```scala
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // ��ǰWatchers�е���ʱ��������������ʱ�临�Ӷ�ΪO(n),���Ծ�����ʹ��isEmpty().
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T) {}
  	
    // �����б�������������еĶ�ʱ����
    def tryCompleteWatched(): Int = {}
    //ȡ������
    def cancel(): List[T] = {}
    // �����б�����Ѿ��������߳���ɵĲ�����
    def purgeCompleted(): Int = {
      
  }
```

> class WatcherList

```scala
 /**
   * ��Key��Watchers��ӳ�䡣�ײ�ʹ��CurrentHashMapʵ�֡�
   */
private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * �������е�Watchers�����ص�Watchers���ܻᱻ�����߳�ɾ��
     */
    def allWatchers = {
      watchersByKey.values
    }
}
```

> def tryCompleteElseWatch

�˷���ΪDelayedOperationPurgatory�ĺ��ķ���֮һ�����ڼ��ö�ʱ�����Ƿ������ɣ���������ɡ�

- һ����ʱ�������ܱ����key��أ���һ����ʱ�������ܻ�����ڶ��Watchers�С�
- ����ĳЩ��ʱ�����������ڼ��벿��Watchers��ʱ�ͱ���ɣ�������Щ�������ᱻ��Ϊ�Ѿ���ɣ��Ҳ�����ӵ�ʣ���Watchers�С�
- �����ո����߳�(expirationReaper)��Ӵ��ڸò���������Watchers �б���ɾ���ò�����

```scala
   /**
   * @param operation ��Ҫ������Ƿ����ɵ���ʱ����
   * @param watchKeys ����ʱ����������������keys
   * @return �����Ƿ��Ǵ˷�������������ɵ���ʱ����
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // tryComplete()�����Ŀ���һ����keys�����������ȡ�
    // ����кܶ�key,Ϊÿ��key����tryComplete()�����ĳɱ���ܴ�
    // �����������������߼���������������tryComplete()������
    // �������û����ɣ�����ֻ��Ҫ�Ѳ�����ӵ�����Key��Ӧ��Watchers�У�
    // Ȼ���ٴε���tryComplete()��
    // ��ʱ�������δ��ɣ����ǿ��԰����������е�key������Watchers�С�
    // ����������ʱ�����Ͳ�����δ�����κδ���������
    // ���ǣ�Ҳ��һ���׶ˣ�����������̼߳��δ��ɣ�����ʱ���������߳���ɣ�
    // �����߳��ֽ��ò�����ӵ�������key������Watchers�С�
    // �������С����,�������,��ΪexpirationReaper�̻߳ᶨ�����������

    // ��ʱ����һ�γ���,��δ�������е�Watchers��,�ܳ�����ɴ˲�����ֻ�е�ǰ�߳�,
    // �����߳��ǰ�ȫ��,ֻ�����trycomplete()��--��һ�ε���tryComplete()
    var isCompletedByMe = operation.tryComplete()
    //��������Ѿ���ɣ�����true���ɡ�
    if (isCompletedByMe)
      return true


    //���δ���


    var watchCreated = false

    //�����е�key���б���,���watcherKeys�б������е�key��δ��ɡ����������key��Ӧ��Watchers��
    for(key <- watchKeys) {
      // ��������Ѿ���ɣ�˵���Ǳ������߳���ɣ���ֹͣ��ʣ���Watchers�б�����Ӵ���ʱ���񡣲�����false
      if (operation.isCompleted)
        return false

      //���δ���,����뵽��ǰkey��Ӧ��Watchers��
      watchForOperation(key, operation)

      //�����е���ʱ��������1
      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }


    //�ٴε���tryComplete()���ڴ�ʱ��ʱ������ܴ�����watchers�У������ܱ������߳�����ɡ�
    //Ҫ����maybeTryComplete()����֤�̰߳�ȫ��ִ����ʱ���� --�ڶ��ε���tryComplete()
    isCompletedByMe = operation.maybeTryComplete()
    //����ɣ������̰߳�ȫ��ʽ�µ���ɣ������ǵ�ǰ�߳���ɵġ��췵��true.
    if (isCompletedByMe)
      return true

    //�����δ���,������뵽ʱ�����С�
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      //�ٴμ���Ƿ����,��������ʱ������ȡ��
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }
```

> def checkAndComplete

У��ĳ��key��Ӧ����ʱ�����Ƿ������ɣ�������ԣ�����ɡ� 

```scala
  /**
   * @return �˴���ɵ���ʱ��������
   */
  def checkAndComplete(key: Any): Int = {
    //�ȴ�WatcherLists��ȡ����Ӧ��WatcherList
    val wl = watcherList(key)
    //watchersList�ж�Ӧ��Watchers
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    //��ͼ������еĲ��� �������������
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }
```

#### ��ʱ����Ӧ��֮��ʱ����

kafka�����߿ͻ��˷�����Ϣʱ�����acks�Ĳ���Ϊ-1����ô����ζ��Ϣ����kafka֮��Ҫ��ISR�����и�������ȡ����Ϣ�󣬲����յ�kafka����˵���Ӧ��

**DelayedProduce��ʱ����**

�ڷ�����յ�KafkaProduce������ʱ:

> kafkaApis.scala

```scala
case ApiKeys.PRODUCE => handleProduceRequest(request)
```

kafka����˻����`replicaManager.appendRecords()`����appendRecords()�����У�����д���DelayedProduce:

```scala
val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
```

Ȼ��delayedProduce����delayedProducePurgatory�С�

```scala
delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
```

DelayedProduce�̳���DelayedOperation�����ȣ�����ÿ��������acksPending��ǳ�ʼ����

```scala
produceMetadata.produceStatus.foreach {
    case (topicPartition, status) =>
    //���û������Error
    if (status.responseStatus.error == Errors.NONE) {
      status.acksPending = true
      // Ԥ����ERROR����,��ʱʱ����ISR�������ͬ��,�������״̬��
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      status.acksPending = false
}
```

DelayedProduceʵ����DelayedOperation�е�tryComplete()�������˷���У�����Ƿ�����һ������������ִ������ʱ������forceComplete()ǿ�������ʱ����ִ������Ϊ��

- ��Broker������Leader������Error���������ʱ����
- ISR�����и��������ͬ����
- ����ͬ���г�����Error��

```scala
  override def tryComplete(): Boolean = {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // ������Щ�Ѿ�������ķ���
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>
            if (partition eq ReplicaManager.OfflinePartition)
              (false, Errors.KAFKA_STORAGE_ERROR)
            else
              partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            //��broker������leader
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // ��Ӧ�г���error||�˷���leader������HW����requiredOffse�����и�����ͬ�����
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }
    //���ȫ��������acksPending����Ƿ�Ϊfalse,
    //��ȫ�������Ƿ�����A����broker������leader����B����leader������HW����requiredOffse||��Ӧ�г���Error
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      //ǿ�������ʱ���񣬼�������Ӧ���ͻ���
      forceComplete()
    else
      false
  }
```

��tryComplete()�����У������з�����������֮�󣬵�����forceComplete()����ǰ����������forceComplete()�лᾫ׼����һ��onComplete()���˷�����DelayedProduce�е�ʵ��Ϊ��

```scala
override def onComplete() {
    //Ϊÿ������������Ӧ״̬
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    //��Ӧ�ص�,������Ӧ״̬��
    responseCallback(responseStatus)
  }
```

**��ʱ������������ͼ**��

![](C:\Users\EDZ\Desktop\�½��ļ���\picture\DelayedProduce��ʱ��������ͼ.png)

#### ��ʱ����Ӧ��֮��ʱ��ȡ

?		��follower���������˷������󣬴�ʱfollower�Ѿ�ͬ������Leader��������Ϣλ�ã��ظ��Ŀ���ȡ�˷���

Դ�����ԣ�kafka����ȡ������Ϣ��Сֵfetch.min.bytes��ʱ�򣬻���д�����ʱ��ȡ������ʱ��ȡ����������

ʱ��������һ�£���ʱ��ȡ��Ϊ����ͬ��Leader����ȡ���������ߵ���ȡ��kafka��������յ���ʱ��ȡ������

ʱ����ȡ��ϢС��fetch.min.bytes����ᴴ��һ��DelayedFetch����ʱ���񣬷���DelayedOperationPurgatory

�С�

��DelayedFetch��ͬ���̳���DelayedOperation����ʵ����def tryComplete()��������������������֮һʱ����

��forceComplete()������

1. ��Broker����������ͼ��ȡĳЩ������Leader Broker��
2. ��Broker�Ҳ�������ͼ��ȡ��ĳЩ������Ϣ��
3. ��ȡ��ƫ����������־�����һ���ֶΡ�
4. �ӷ�������ȡ����Ϣ��������С�ֽڡ�
5. ����λ�ڴ˴����������־Ŀ¼�С�
6. Broker����Leader,�������epoch�������ˡ�

> DelayedFetch.scala
>
>  override def tryComplete()

```scala
  /**
   * ��ɺ󣬷���ÿ����������Ч����
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    //����ÿ������������
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        //��ȡ��ȡ��Ϣ����ʼƫ����
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val partition = replicaManager.getPartitionOrException(topicPartition,
              expectLeader = fetchMetadata.fetchOnlyLeader)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)
            //��ȡ��־ƫ���������а���HWλ�ã�LOG�����һ����
            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            //��һ����ȡ��ƫ�����Ƿ����仯,��������仯,��˵������������Ϣд�롣�л�����ȡ������
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                //endOffset�����fetchOffsetλ�ڸ�old����־�ֶ��У�
                //���ܸո�Leader崻��������µ�Leader��������־�ضϡ���������3.��Ϊ��ǰ��Ծ����־�ֶλ���endOffset���ڵ���־�ֶΡ�
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                //Ҫ��ȡ��λ�Ʋ���activSegment�С�����endOffsetȴ��activeSegment�У����ܵ�ǰ�����ո��������µ�activeSegment�ͻᷢ���������
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                //Ҫ��ȡ��λ�ƺ����һ����Ϣλ����ͬһ����־�ֶ��У���ʱ�Ա�����λ�ƣ����Ҫ��ȡλ��С��endOffset�����ۼ�accumulatedSize�ֽ�����
                //���������ĸ���
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }
          }
        } catch {
          case _: KafkaStorageException => // ����5 ��־����������־Ŀ¼�У����������ڲ�����״̬��
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // ����2 Broker�Ҳ�����Ҫ��ȡ��������Ϣ��
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException =>
            // ����6 Ϊ��ֹ��Ϣ��ʧ��kafka������Ϣ�汾��Leader Epoch,
            // Ҳ������ȻBroker�Ƿ�����Leader Broker��������İ汾���Ƿ��������°汾�ţ����������������������ʱ��ȡ��
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // ����1,��Broker������Leader��
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }
```





















## �����֮������

## �����֮�������