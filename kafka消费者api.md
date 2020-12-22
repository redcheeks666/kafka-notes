# kafka消费者API

* [kafka消费者API](#kafka消费者api)
  * [消息订阅](#消息订阅)
  * [取消订阅](#取消订阅)
  * [消费拉取](#消费拉取)
  * [控制消费](#控制消费)
  * [位移提交](#位移提交)
    * [自动提交](#自动提交)
    * [手动提交](#手动提交)
      * [手动同步提交](#手动同步提交)
      * [手动异步提交](#手动异步提交)
  * [多线程消费](#多线程消费)
    * [线程封闭方式](#线程封闭方式)
    * [多线程消费同一分区方式](#多线程消费同一分区方式)
    * [多线程消息处理方式](#多线程消息处理方式)

    ​

## 消息订阅

```java
//@topics 主题集合
void subscribe(Collection<String> topics);
//@callback 设置相应的在均衡监听器
void subscribe (Collection < String > topics, ConsumerRebalanceListener callback);
```

```java
//@pattern 规则订阅 以正则表达式订阅 如 consumer.subscribe(Pattern.compile("topic_.*"));
void subscribe (Pattern pattern);
//@callback 设置相应的在均衡监听器
void subscribe (Pattern pattern, ConsumerRebalanceListener callback);
```

```java
//@partitions 指定分区集合 TopicPartition { topic partition }
void assign (Collection < TopicPartition > partitions);
```

可用assign来精准分区订阅，如下

```java
//手动指定分区，订阅topic为"topic-demo"，0分区
consumer.assign(Arrays.asList(new TopicPartition("topic-demo",0)));
```

KafkaConsumer还提供了partitionsFor()方法，获取topic的分区信息。

```java
List<PartitionInfo> partitionsFor(String topic);
List<PartitionInfo> partitionsFor(String topic, Duration timeout);
```

如实现全部topic分区订阅:

```java
//        分区集
List<TopicPartition> partitions = new ArrayList<>();
//        获取当前主题分区信息
List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
//        订阅当前主题的全部分区消息
if (partitionInfos!=null){
   for (PartitionInfo partitionInfo : partitionInfos) {partitions.add(newTopicPartition(partitionInfo.topic(),partitionInfo.partition()));}
}
consumer.assign(partitions);
```

subscribe()订阅时，具有消费者再均衡，消费者增加减少时，自动实现分区分配平衡，分区分配关系重新分配，实现负载均衡，故障转移。

assgin()订阅时，不具备消费者自动均衡。

## 取消订阅

```java
//取消订阅:(三个方法实现效果相同)
consumer.unsubscribe();
consumer.subscribe(new ArrayList<String>());
consumer.assign(new ArrayList<TopicPartition>());
```

## 消费拉取

对于Kafka的消费模式，官方文档是这么说的:

> 源码翻译过来总结的:

```tex
1.数据从Producer到broker的模式是Push,之后Consumer从broker中pull数据。(传统方式)
2.Flume这些不一样，push直接到下游节点，两种方式各有千秋
3.broker控制数据传输速率，push模式很难处理不同consumer，broker的目标是让consumer实现最大效率消费，然而,当生产率>>消费率,Push中的消费者便不堪重负。pull模式在此时有一个优势便是消费者可以在后面适当的时间赶上来。或者通过backoff(一种重试算法)协议避免减少这种状况。
4.Pull模式还有一种优点，消息可直接批量发送，消费者适当的时候拉取，Push在不知道下游消费情况的状态下，只能在单个和批量直接做选择，如果做延迟处理，那么数据处理就会引入不必要的延迟。
5.pull的缺点，如果broker没有数据，消费者会在一个紧密的循环中等待数据。为了避免这种不停地循环，在pull请求中增加了一些参数，如阻塞时长timeout,直到数据到来。
6.如果全部端到端的都是基于pull，消息到log，broker从Producer那拉，consumer从broker那拉，大规模数据化中不可靠，操作麻烦，作者觉得不适合，我们发现，大规模的运行的SLA管道，省略Producer的持久化。
```

客户端poll:

```java
ConsumerRecords<K, V> poll(long timeout);//已经过期
ConsumerRecords<K, V> poll(Duration timeout);
```

timeout	poll()方法拉取到非空数据集的阻塞时间,设置0则为不阻塞，无论是否拉取到消息直接返回。如果线程只是为了拉取消息，可设置为最大值，Long.MAX_VALUE

poll()返回的是一个迭代器,也就是一个消息实例集

```java
Iterable<ConsumerRecord<K, V>>
```

下面poll方法的源码

```java
private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
	//确定此对象是单线程进入.与producer不同consumer线程不安全 否则抛异常throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
	acquireAndEnsureOpen();
	try {
		//判断KafkaConsumer是否订阅了topic
		if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
			throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
		}
		// poll for new data until the timeout expires
		//一直拉数据直到超时 时间设置也就是poll()中阻塞时长参数
		do {
			client.maybeTriggerWakeup();
			if (includeMetadataInTimeout) {
				if (!updateAssignmentMetadataIfNeeded(timer)) {
					return ConsumerRecords.empty();
				}
			} else {
				while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE))) {
					log.warn("Still waiting for metadata");
				}
			}
		//拉取一次消息的方法,它会先判断有上次是否有已经拉取到,但是未加工的消息.如果没有则创建请求，网络IO等拉取消息
			final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
			if (!records.isEmpty()) {
				//!records.isEmpty()如果消息不为空 不会立即返回，准备下一次拉取，
				//并且使用的非线程阻塞方式，异步操作，类似producer中的sender线程
				//这样做,等下次消息拉取的时候,j就会有未操作已拉取的消息存在缓存,省去网络IO的时间.提高效率
				//fetcher.sendFetches() 拉取请求准备,返回值> 0 准备请求成功
				//client.hasPendingRequests() 是否有挂起的请求.
				if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
					client.pollNoWakeup();
				}
			return this.interceptors.onConsume(new ConsumerRecords<>(records));
			}
		} while (timer.notExpired());
	return ConsumerRecords.empty();
	} finally {
		release();
	}
}
```

说下pollForFetches()，从服务端拉取消息的动作，完成了下面几个动作

1. 查看是否已存在拉取回来未处理的数据，有的话立即调用fetcher.fetchedRecords()加工，然后返回。
2. 没有已经拉取的数据，调用fatcher.sendFetches()准备拉取消息的请求。
3. 通过ConsumerNetworkClient发送拉取请求
4. 加工拉取的数据，返回。

Fetcher负责准备拉取消息的Request,处理Response。不负责IO，由ConsumerNetworkClient负责IO。

ConsumerNetworkClient是对NetworkClient的封装，实现网络IO，发送请求，处理返回。

> 以下是ConsumerRecord的结构

```java
public class ConsumerRecord<K, V> {
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;
    public static final int NULL_CHECKSUM = -1;

    private final String topic;//topic
    private final int partition;//分区
    private final long offset;//所属分区位移
    private final long timestamp;//时间戳
    //时间戳类型
    //1.CreateTime 消息创建时间
    //2.LogAppendTime 追加到日志的时间
    private final TimestampType timestampType;
    private final int serializedKeySize;//key序列化后大小，key为null serializedKeySize=-1
    private final int serializedValueSize;//value序列化后大小，value为null serializedValueSize=-1
    private final Headers headers;//消息头部内容
    private final K key;//
    private final V value;//
    private final Optional<Integer> leaderEpoch;//

    private volatile Long checksum;
    ...
}
```

ConsumerRecord提供了一个获取指定分区的消息集

```java
public List<ConsumerRecord<K, V>> records(TopicPartition partition)
```

还有获取指定topic的消息集

```java
public Iterable<ConsumerRecord<K, V>> records(String topic) 
```

统计消息个数的方法

```java
public int count()
```

判断消息是否为空

```java
//等效直接判断ConsumerRecord是否为空
public boolean isEmpty() {return records.isEmpty();}
```

## 控制消费

控制消费的方法,KafkaConsumer提供了三个方法

```java
//暂停消费的分区集
Set<TopicPartition> paused();
//暂停消费分区
void pause(Collection<TopicPartition> partitions);
//回复分区消费
void resume(Collection<TopicPartition> partitions);
```

对于poll()外层的while()循环，三种控制方式

```java
private static final AtomicBoolean isRunning=new AtomicBoolean(true);
while(isRunning.get()){}
//退出循环
isRunning.set(false)
```

```java
while(true){}
```

```java
//退出循环 KafkaConsumer唯一可以从其他线程安全调用方法
KafkaConsumer.wakeup();
```

跳出循环后，关闭相关资源，内存，Sorket连接等

```java
void close();
@Deprecated
void close(long timeout, TimeUnit unit);
void close(Duration timeout);
```

kafkaConsumer提供的一个seekToBeginning()方法从每个分区起始位置开始消费。

```java
Set<TopicPartition> assignment = new HashSet<>();
//确保poll执行后能拿到分区分配的信息
	do {
		kafkaConsumer.poll(1000);
		assignment = kafkaConsumer.assignment();
	}while (assignment.size()==0);
//分区起始处消费
assignment.forEach(x->kafkaConsumer.seekToBeginning(Arrays.asList(x)));
```

从每个分区末尾消费

```java
//kafkaConsumer提供的endOffsets方法 获取每个分区末尾的位移
Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);
//seek指定末尾位移消费
assignment.forEach(x->kafkaConsumer.seek(x,endOffsets.get(x)));
//循环拉取消息
while (isRunning.get()){
	ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
	for (ConsumerRecord<String, String> record : records) {
	System.out.println(record.value());
	}
}
```

同样，kafkaConsumer提供的一个seekToEnd()方法。

```java
Set<TopicPartition> assignment = new HashSet<>();
//确保poll执行后能拿到分区分配的信息
	do {
		kafkaConsumer.poll(1000);
		assignment = kafkaConsumer.assignment();
	}while (assignment.size()==0);
//分区末尾处消费
assignment.forEach(x->kafkaConsumer.seekToEnd(Arrays.asList(x)));
```

当然，seek方法中不仅可以从始末位置，还可以从任意位置。第一个参数分区，第二个参数填入想开始消费的位移位置便可。

除了可以从指定位置开始消费，还可以从指定时间戳开始，比如用户想消费前一天的数据。可以通过kafkaConsumer提供的offsetsForTimes()方法拿到大于传入时间戳的每个分区第一条消息位移位置

> offsetsForTimes()方法定义如下

```java
Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);
```

从分区的某一个时间节点开始消费：

```java
public static void main(String[] args) {
	Properties properties = initProperties();
	KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
	kafkaConsumer.subscribe(Arrays.asList(topic));
    
	//获取分区信息
	Set<TopicPartition> assignment = new HashSet<>();
	do {
		kafkaConsumer.poll(1000);
		assignment=kafkaConsumer.assignment();
	}while (assignment.size()==0);
    
	//构建待查询时间戳map 时间戳是一天前
	HashMap<TopicPartition, Long> timeStamp = new HashMap<>();
	assignment.forEach(x->timeStamp.put(x,System.currentTimeMillis()-1*24*60*60*1000L));
	//通过时间戳拿到大于时间戳的第一条消息的位移
	Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(timeStamp);
	//拿到分区集
	Set<TopicPartition> topicPartitionSet = offsets.keySet();
	//seek指定消费
	topicPartitionSet.forEach(x->{if(offsets.get(x)!=null){kafkaConsumer.seek(x,offsets.get(x).offset());}});
	
    //开始消费
	ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
	for (ConsumerRecord<String, String> record : records) {
		System.out.println(record.value());
	}
}
```

位移提交了，但是当有新的消费者产生，或者消费者组订阅了新的topic，在__consumer_offsets中查不到位移信息，也可能位移已过期删除，总之当kafka消费者找不到位移信息时，该如何？

kafka消费者客户端提供了一个参数auto.offset.reset

> auto.offset.reset=latest (默认)从分区末尾开始消费

> auto.offset.reset=earliest 从分区起始处消费

> auto.offset.reset=none 查不到位移抛异常NoOffsetForPartitionException

## 位移提交

### 自动提交

```shell
auto.commit.offset=true
```

调用poll()后，每隔5秒提交本次拉取数据的最大位移，每次poll()方法被调用时，判断是否到达提交时间，到达时间则提交上次poll()返回的最大位移

> 自动提交导致的消息丢失和重复消费

重复消费-消费者poll()方法执行后，还未到达延时提交时间，消费者宕机，或者消费者发生重平衡，位移就未提交，再次消费就会重复消费

消息丢失-消费者poll()方法执行后，到延时提交时间，位移提交，但缓存到本地的消息未处理完成，便造成了消息丢失

### 手动提交

```shell
auto.commit.offset=false
```

手动提交位移又分为同步提交commitSync()和异步提交commitAsync()

#### 手动同步提交

```java
while (true){
	ConsumerRecords<String, String> records = conseumer.poll(1000);
	for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                    System.out.println(partitionRecord.value());
	}
    //位移同步提交
    conseumer.commitSync();
}
```

还可以同步批量提交(提交时会发生阻塞，批量提交可减少提交频率)，这样

```java 
final int minBatchSize=30;
List<ConsumerRecord> list = new ArrayList<>();
while (true){
	ConsumerRecords<String, String> consumerRecords = conseumer.poll(Duration.ofMillis(1000));
	for (ConsumerRecord<String, String> record : consumerRecords) {
					System.out.println(record.value());
					list.add(record);
	}
    //消息数量达到30条提交
	if (list.size()>=minBatchSize){
	conseumer.commitSync();
	//缓冲区清零
    list.clear();
	}
}
```

对于同步提交还可以

```java
//精准提交，精确某个分区进行位移提交
void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
```

同步批量提交可以减少提交频率，但会增加重复消费的几率，而且会线程阻塞，kafka还提供了异步手动提交

#### 手动异步提交

就在commitSync下面，kafka定义了三个异步提交的方法

```java
/**
* @see KafkaConsumer#commitAsync()
*/
void commitAsync();
/**
* @see KafkaConsumer#commitAsync(OffsetCommitCallback)
*/
void commitAsync(OffsetCommitCallback callback);
/**
* @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
*/
void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);
```

比同步commitSync多了一个OffsetCommitcallback回调函数，位移提交完成以后，会回调触发Offsetcommitcallback中的onComple()方法

在程序退出，消费者发生重平衡时，希望可以最终保底提交一次位移

**同步异步混合提交**

```java
try {
	while (isRunning.get()) {
		ConsumerRecords<String,String> records = consumer.poll(1000);
		for (ConsumerRecord<String, String> consumerRecord : records) {
                 System.out.println(consumerRecord.value());
		}
    	//异步提交
		consumer.commitAsync(new OffsetCommitCallback() {
		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				if (exception==null){
					System.out.println(offsets.get(topicPartition).offset());
				}else {
					System.out.println("提交位移失败");
				}
			}
		});
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
        //同步提交 消费者正常退出后 保底提交位移
		consumer.commitSync();
	}
}
```

对于异步提交。提交失败后的重试会造成重复消费,比如第一次提交位移100失败了，第二次提交150成功了，此时第一次开始重试，提交n成功了，位移就会变为100，消息重复消费。

**解决方案**：

使用单调递增的序号，每次异步提交，增加序号相应的值，每次提交成功后，对比当前提交位移与序号值，位移<自增序号，已有最新的位移提交，无需重试，位移=自增序号，可重试。

> 控制消费的方法,KafkaConsumer提供了三个方法

```java
//暂停消费的分区集
Set<TopicPartition> paused();
//暂停消费分区
void pause(Collection<TopicPartition> partitions);
//回复分区消费
void resume(Collection<TopicPartition> partitions);
```

> 对于poll()外层的while()循环，三种控制方式

```java
private static final AtomicBoolean isRunning=new AtomicBoolean(true);
while(isRunning.get()){}
//退出循环
isRunning.set(false)
```

```java
while(true){}
```

```java
//退出循环 KafkaConsumer唯一可以从其他线程安全调用方法
KafkaConsumer.wakeup();
```

> 跳出循环后，关闭相关资源，内存，Sorket连接等

```java
void close();
@Deprecated
void close(long timeout, TimeUnit unit);
void close(Duration timeout);
```

位移稳稳的提交了，但是当有新的消费者产生，或者消费者组订阅了新的topic，在__consumer_offsets中查不到位移信息，也可能位移已过期删除，总之当kafka消费者找不到位移信息时，该何去何从？

kafka消费者客户端提供了一个参数auto.offset.reset

> auto.offset.reset=latest (默认)从分区末尾开始消费

> auto.offset.reset=earliest 从分区起始处消费

> auto.offset.reset=none 查不到位移抛异常NoOffsetForPartitionException

## 多线程消费

**kafka的Producer是线程安全的，但Consumer是线程不安全的。**KafkaConsumer中除wakeup()方法外，所有方法在执行前都会调用一个 acquire(),此方法是检测是否只有一个线程在操作。如果有其他线程则抛出异常。

acquire()与synchronized、Lock等不同，他不会造成线程阻塞，可以看成一个轻量级的锁。通过进行线程操作计数的方式来判断线程是否进行了并发操作。除了acquire()‘加锁’的方法，对应还有一个release()'解锁'的方法，两个方法都是私有的，内部调用保证单线程操作。

虽然Consumer是线程不安全的，但是在某些场景下，我们需要多线程进行拉取，避免上游数据量过大，下游消费不及时，造成的消息丢失，也避免消费延迟，我们不得不通过多线程的方式来提高消费能力。

### 线程封闭方式

线程封闭，为每一个线程实例化一个Consumer。

缺点：一个线程对应一个消费者，一个消费者订阅一个或多个分区，所有线程隶属于同一个消费者组，默认情况下，同一个分区只能对应同一个消费者组的一个消费者，如果组内消费者数量>分区数，就会有空闲的消费者消费不到消息，当多线程时，就会有空闲的线程。

分区数多时，TCP连接的资源开销大。

优点：类似于开启多个Consumer，每个线程可以按顺序消费各个分区的消息。

示例：

```java
public class EveryOneThreadForConsumer {
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        //开启三个线程
        int ThreadCount=3;
        for (int i = 0; i < 3; i++) {
           new KafkaThread(properties,KafkaProperties.TOPIC).start();
        }
    }

    public static class KafkaThread extends Thread{
        private KafkaConsumer<String,String> kafkaConsumer;
        //每个线程构建独立的KafkaConsumer
        public KafkaThread(Properties properties,String topic ){
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                
                for (ConsumerRecord<String, String> record : records) {
                    //处理消息
                    System.out.println(record);
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
```

### 多线程消费同一分区方式

即通过assign(),seek()方式实现，一个分区多个线程消费，一般来说，分区是消费线程的最小单位，通过assign()指定分区进行订阅，seek()实现从分区指定位置消费。对于位移和消费顺序控制变得复杂。

### 多线程消息处理方式

poll()拉取的消息的速度是很快的，消费处理消息是Kafka消费的瓶颈点，对于消费慢，我们可以通过提升消息拉取频率(提高处理速度)来提高消费。

但此方式对于消息位移的控制，消息消费顺序难以控制。

我们可以针对每一个分区设置一个所有线程共享的偏移量(`Map<TopicPartition, OffsetAndMetadata>`)，获取拉取到消息集每个分区最后一条偏移量，与共享对应分区偏移量对比，共享中没有则直接保存。如果当前分区共享偏移量小于当前分区消息集的最后一条消息的下一条偏移量。则进行位移更新。反之则不进行对共享偏移量操作。每次拉取到消息提交给处理线程后，清空共享偏移量。

```java
public class EveryOneThreadHandle {
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    private static final Map<TopicPartition, OffsetAndMetadata> offsets=new HashMap<>();

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        //手动提交位移
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }
    //此处可横向扩展  开启多个kafkaConsumerThread 来进一步提升消费
    /**for (int i = 0; i < 5; i++) {
     *   new KafkaConsumerThread().start();
     *   }
     */
    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, KafkaProperties.TOPIC, Runtime.getRuntime().availableProcessors());

        kafkaConsumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread{
        private KafkaConsumer<String,String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNum;


        public KafkaConsumerThread(Properties properties,String topic ,int threadNum) {
            KafkaConsumer<String, String> KafkaConsumer = new KafkaConsumer<>(properties);

            kafkaConsumer.subscribe(Arrays.asList(topic));
            this.threadNum=threadNum;


            executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        }

        @Override
        public void run() {

            try {
                while (isRunning.get()){
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()){
                        executorService.submit(new RecordHandle(records));

                        synchronized (offsets){
                            if (!offsets.isEmpty()){
                                kafkaConsumer.commitSync(offsets);
                                offsets.clear();
                            }
                        }

                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }

        }
    }

    public static class RecordHandle extends Thread{

        public final ConsumerRecords<String, String> records;


        public RecordHandle(ConsumerRecords<String, String> records){
            this.records=records;
        }


        @Override
        public void run(){

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecordList = this.records.records(partition);
                //获取最后一条位移
                long lastOffset = consumerRecordList.get(consumerRecordList.size() - 1).offset();

                synchronized (offsets){

                    //在这里,将每个分区的位移缓存在offsets中共享。在poll方法后取出提交。
                    if (!offsets.containsKey(partition)){
                        offsets.put(partition,new OffsetAndMetadata(lastOffset+1));
                    }else {
                        long partitionOffset = offsets.get(partition).offset();
                        //如果共享变量中的位移 小于当前消费的位移 则更新共享的位移(正常位移顺序消费)
                        //反之，则不对共享位移进行操作，此时消费的位移属于共享位移之前的消息。
                        if (partitionOffset < lastOffset+1){
                            offsets.put(partition,new OffsetAndMetadata(lastOffset+1));
                        }
                    }

                }


            }
            //处理消息
            System.out.println(records);
        }


    }


}
```

上面，对比共享位移和当前位移，保证了位移不会被覆盖，就类似于，消费50-100的位移可能会被0-50的位移进行覆盖，(拉取到位移为0-50的消费较慢)，但无法保证消息丢失的情况，(消费50-100的位移提交后，0-50的位移消费失败，重试时无法再次消费到0-50的消息)。

对于这种情况，我们可以使用窗口的概念来试图解决。

消费者拉取到消息后，将消息暂存起来，暂存大小为固定批次的范围。比如固定为五个批次的消息。

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Consumer窗口消费模型.png)

图中每个方格代表拉取到的一个消息批次。

startOffset是窗口起始批次的起始位移。

endOffset是结束批次末尾位移。

处理线程从暂存的窗口中拉取消息集。我们只关注窗口中的起始批次。窗口所有批次并行消费，每次起始批次中的消息消费完成，便提交起始批次的位移，并删除空口中此批次的消息。此时窗口向前滑动一格。

如果起始批次中的消息一直无法消费完成提交位移，就会造成窗口悬停(长时间不向前滑动),消费也随即停止。为了避免这种情况，我们就需要设定一个条件，当消费一定时间内无法消费提交位移时，就需要重试，或将起始批次中的消息投递进死信队列，让窗口继续向前滑动。还可以开启另外一个线程，来定时维护，处理死信队列中的消息。以便进行调整。

此方案中，方格大小或者窗口大小觉定了并发度，一格方格对应一个消费处理线程，方格或窗口太大，增大开销，故障恢复引起大量重复消费，太小，则并发度不够。应合理配置。





[^]: 本文参考自《深入理解Kafka：核心设计与实践原理》









