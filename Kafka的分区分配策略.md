# kafka的分区分配策略

* [kafka的分区分配策略](#kafka的分区分配策略)
  * [kafka提供的分配策略](#kafka提供的分配策略)
    * [AbstractPartitionAssignor](#abstractpartitionassignor)
    * [PartitionAssignor](#partitionassignor)
    * [三个kafka提供的分区策略:](#三个kafka提供的分区策略)
      * [RangeAssignor](#rangeassignor)
      * [RoundRobinAssignor](#roundrobinassignor)
      * [StickyAssignor](#stickyassignor)
    * [自定义分配方案](#自定义分配方案)
      * [自定义实现随机分配方案](#自定义实现随机分配方案)
      * [自定义组内广播策略](#自定义组内广播策略)

      ​

## kafka提供的分配策略

Kafka对用户提供三个分区的方案，

在Kafka源码org.apache.kafka.clients.consumer包下，有三个分配的文件

> RangeAssignor.java(默认)

> RoundRobinAssignor.java

> StickyAssignor.java

![分区实现关系图](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\分区实现关系图.png)

### AbstractPartitionAssignor

三个都继承了AbstractPartitionAssignor抽象类，AbstractPartitionAssignor定义如下，实现了PartitionAssignor接口：

```java
public abstract class AbstractPartitionAssignor implements PartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /***
     *
     * @param partitionsPerTopic topic-topicPartitionCount
     * @param subscriptions memeberId-Subscription
     * @return
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);


    @Override
    public Subscription subscription(Set<String> topics) {
        //此处topic订阅信息类使用的是未带用户自定义信息的构造
        //还可在此加入用户自定义的信息,构造Subscription 如权重,ip,host,rack机架信息等
        //让消费者与消费的分区所在的broker尽可能分在同一机架
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        //把所有消费者订阅的主题收集
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());
        //统计topic 与对应分区的数量的关系
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            //获取每个topic的分区数
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }
        //子类的分配实现逻辑,去除集群的元数据
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

        // 不维护用户数据,封装结果
        Map<String, Assignment> assignments = new HashMap<>();
        //<String, List<TopicPartition>> 到<String, Assignment> 的转换
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return assignments;
    }

    /***
     * 消费者重平衡,在消费者Leader收到GroupCoordinator的分配信息进行分配后,调用此方法
     * 可以不做任何实现
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, Map)}
     */
    @Override
    public void onAssignment(Assignment assignment) {
        // this assignor maintains no internal state, so nothing to do
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }
```

### PartitionAssignor

```java
public interface PartitionAssignor {
    /***
     *  1.在消费者重平衡的第二阶段,创建JoinGroupRequest请求时,会回调此方法
     *  2.可在此方法内，添加影响分配的用户自定义信息，如:权重,ip,host,rack机架信息等
     * @param topics 主题信息
     * @return 可序列化的主题订阅信息,可包含其他例如机架信息等
     */
    Subscription subscription(Set<String> topics);

    /**
     * 真正的分配方案实现方法
     *
     * @param metadata 消费者已知的当前主题/broker的元数据
     * @param subscriptions 所有成员的订阅信息 memberId-Subscription{@link #subscription(Set)}
     * @return
     */
    Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions);

    /**
     * 在消费者冲平衡的第三阶段,消费者Leader收到分配方案进行分配后回调。
     * 如在kafka提供的StickyAssignor分配方案中,使用此方法保存当前的分配方案
     * 也可不实现此方法
     * @param assignment 分配方案
     */
    void onAssignment(Assignment assignment);


    default void onAssignment(Assignment assignment, int generation) {
        onAssignment(assignment);
    }


    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky")
     * @return non-null unique name
     */
    String name();

    /***
     * 消费者的订阅主题列表和用户自定义信息userData
     */
    class Subscription {
        private final List<String> topics;
        private final ByteBuffer userData;

        public Subscription(List<String> topics, ByteBuffer userData) {
            this.topics = topics;
            this.userData = userData;
        }

        public Subscription(List<String> topics) {
            this(topics, ByteBuffer.wrap(new byte[0]));
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                    "topics=" + topics +
                    ')';
        }
    }

    /***
     * 消费者分配到的分区集合partitions
     * 用户自定义信息userData
     */
    class Assignment {
        private final List<TopicPartition> partitions;
        private final ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, ByteBuffer.wrap(new byte[0]));
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                    "partitions=" + partitions +
                    ')';
        }
    }

}
```

### 三个kafka提供的分区策略:

#### RangeAssignor

```java
package org.apache.kafka.clients.consumer;
public class RangeAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "range";
    }
    //返回每个topic与被订阅的消费者映射关系
    private Map<String, List<String>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        //consumerMetadata所有消费者memberId 订阅的信息
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            //consumerId当前消费者id
            String consumerId = subscriptionEntry.getKey();
            //所有topic
            for (String topic : subscriptionEntry.getValue().topics())
                //put()父类AbstractPartitionAssignor中的方法
                //1.先判断res集合里是否存在key为topic的实例,有:value累加当前消费者Id,无:先创建,后value累加当前消费者id
                put(res, topic, consumerId);
        }
        return res;
    }

    /***
     * RangeAssignor实现分区分配逻辑的方法
     *
     * @param partitionsPerTopic topic-topicPartitionCount
     * @param subscriptions memeberId-Subscription
     * @return
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        //由Map<memberId,List<Topic>>映射关系 转换为Map<topic,List<memberId>>的映射关系
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        //分配结果 memberId-List<topicPartition>
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        //构建key为消费者memberid的map实例
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            //获取当前topic
            String topic = topicEntry.getKey();
            //获取该topic的消费者集合
            List<String> consumersForTopic = topicEntry.getValue();
            //获取该消费者的分区数量
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            //关键分区分配逻辑  ↓
		
            //把该topic的所有消费者memberId按字典顺序排序
            Collections.sort(consumersForTopic);

            //topic的分区数量/订阅该topic的消费者数量(取整)
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            //topic的分区数量%订阅该topic的消费者数量 取余
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

            //获取分区集合,List<TopicPartition> topicPartition（topic partitionNum）
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            //给每一个消费者分配分区
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                //获取起始下标,取整后的值*i + Math.min(i, consumersWithExtraPartition)      
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                 //获取步数长度,取余后的值若大于 i + 1 则 取整后的值 + i + 1, 取余后的值若小于 i + 1 则 取整后的值 + i
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                //为下标为i的消费者 添加分配到的分区信息
                assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }

}
```

举个例子,现在有两个topic(t0,t1),每个topic有4个分区(t1p1,t1p2,t1p3,t1p4,t2p1,t2p2,t2p3,t2p4),两

个消费者订阅这两个topic(c1,c2),那么,按上面代码逻辑分配后的结果为:

```txt
c1:t1p1,t1p2,t2p1,t2p2
c2:t1p3,t1p4,t2p3,t2p4
```

在不考虑业务场景的情况下，这样看似很均衡，但是，当多数的topic的numPartitionsForTopic % consumersForTopic.size()的余数consumersWithExtraPartition不为0时，例如，把上面的两个topic的分区数改为3.按RangeAssignor再次分配后就变为:

```java
c1:t1p1,t1p2,t2p1,t2p2
c2:t1p3,t2p3 
```

字典顺序排名靠前的消费者就会分配更多的分区.负载压力就不再均衡。

#### RoundRobinAssignor

看名字就很明显。这是一个轮寻的策略，将所有消费者和所有订阅的分区字典排序后，轮寻的方式分配给消费者。

```java
package org.apache.kafka.clients.consumer;
public class RoundRobinAssignor extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        //获取所有的消费者id，构建消费者与分区的映射Map准备
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        //Util.sorted方法底层对subscriptions.keySet()做了Collections.unmodifiableList(res)
        //即返回的assigner是不可修改的迭代器 存的是消费者id
        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            //当前topic
            final String topic = partition.topic();
            //轮寻分配
            while (!subscriptions.get(assigner.peek()).topics().contains(topic))
                assigner.next();
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }

    /***
     * 方法的用途如名称所示,获取所有排序后的分区
     * @param partitionsPerTopic topic-partitionCountNum
     * @param subscriptions memeberId-List<topic>
     * @return
     */
    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {

        SortedSet<String> topics = new TreeSet<>();
        //获取排序去重后的所有topic名称
        for (Subscription subscription : subscriptions.values())
            topics.addAll(subscription.topics());

        List<TopicPartition> allPartitions = new ArrayList<>();
        //获取所有topic的所有分区
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }

        return allPartitions;
    }

    @Override
    public String name() {
        return "roundrobin";
    }

}
```

还是之前的两个topic(t0,t1),每个topic有4个分区(t1p1,t1p2,t1p3,t1p4,t2p1,t2p2,t2p3,t2p4),两

个消费者订阅这两个topic(c1,c2)，按RoundRobinAssignor轮寻策略分配的话，结果即:

```java
c1:t1p1,t1p3,t2p1,t2p3
c2:t1p2,t1p4,t2p2,t2p4
```

只要消费者订阅的分区相同，在不考虑业务场景的情况下， 是比较均衡的，但是当消费者订阅的topic不同时,分配便不再均衡,如c1订阅t1,t2 ,c2只订阅了t1,那么分配后的结果即:

```java
c1:t1p1,t1p3,t2p1,t2p2,t2p3,t2p4
c2:t1p2,t1p4
```

#### StickyAssignor

**粘性分配，特点:**

1. **主题分区仍然尽可能均匀地分布**
2. **分配结果尽可能与上次保持一致**

**第一个的权重更高。**

来直观看一下StickAssignor 的分配结果

我们创建三个Topic: t1,t2,t3

每个Topic  2个分区 3个副本

```java
NewTopic t01 = new NewTopic("t1", 2, (short) 3);
NewTopic t02 = new NewTopic("t2", 2, (short) 3);
NewTopic t03 = new NewTopic("t3", 2, (short) 3);
```

三个消费者 C1,C2,C3

StickyAssignment的分配方案为

```txt
C1 : t1p1 t3p1
C2 : t1p0 t2p1
C3 : t2p0 t3p0
```

宕掉一台

```txt
C1 t3P0  t3P1  t1P1
C2 t2P1  t1P0  t2P0
```

可以明显看到，在C3宕机后，C1,C2中依然保留着上次的分配到的分区。

这样很大程度减少了分区与消费者之间的迁移。

StickyAssiment分配的源码可以说非常复杂了。因为分配方案是在客户端生成，如果想了解，无需搭建服务端源码。

### 自定义分配方案

自定义分配可以更加灵活的分配分区。

通过实现PartitionAssignor接口或继承AbstractPartitionAssignor抽象类来配置分配方案，AbstractPartitionAssignor是对PartitionAssignor进一步的封装。

一般来说，继承AbstractPartitionAssignor即可。

#### 自定义实现随机分配方案

```java
public class CustomAssignorPartition extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        HashMap<String, List<TopicPartition>> assignment = new HashMap<>();
        Set<String> consumers = subscriptions.keySet();

        for (String consumer : consumers) {
            assignment.put(consumer,new ArrayList<TopicPartition>());
        }


        Set<TopicPartition> partitions = new HashSet<>();
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            List<String> topics = entry.getValue().topics();
            if (topics==null) continue;
            for (String topic : topics) {
                Integer partnum = partitionsPerTopic.get(topic);
                if (partnum==null) continue;
                for (Integer i = 0; i < partnum; i++) {
                    partitions.add(new TopicPartition(topic,i));
                }
            }
        }
//        for (String topic : subscriptions.keySet()) {
//            Integer partnum = partitionsPerTopic.get(topic);
//            if (partnum==null) continue;
//            for (Integer i = 0; i < partnum; i++) {
//                partitions.add(new TopicPartition(topic,i));
//            }
//        }
        Map<TopicPartition, List<String>> counsumerForPartition = getCounsumerForPartition(partitionsPerTopic, subscriptions);
        //kafka自定义的迭代器
        // CircularIterator<String> iterator = new CircularIterator<String>(new ArrayList<>(subscriptions.keySet()));

        //partitions所有分区
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();
            //判断该消费者是否订阅此topic
            //while (!subscriptions.get(iterator.peek()).topics().contains(topic)) iterator.next();
            //获取订阅该分区的消费者集
            List<String> consumerlist = counsumerForPartition.get(partition);

            if (consumerlist.size()>0){
                //随机分到消费者
                int i = new Random().nextInt(consumerlist.size());
                String consumer = consumerlist.get(i);
                //添加到分配方案中
                assignment.get(consumer).add(partition);
            }


        }

        return assignment;
    }

    @Override
    public void onAssignment(Assignment assignment, int generation) {

    }

    @Override
    public String name() {
        return "RandomAssignor";
    }

    //获取分区与订阅消费者的映射
    public Map<TopicPartition,List<String>> getCounsumerForPartition(Map<String, Integer> partitionsPerTopic,
                                                                     Map<String, Subscription> subscriptions){
        HashMap<TopicPartition, List<String>> consumerForPartition= new HashMap<>();
        //构建分区与消费者的对应关系
        for (Map.Entry<String, Integer> entry : partitionsPerTopic.entrySet()) {
            for (Integer i = 0; i < entry.getValue(); i++) {
                consumerForPartition.put(new TopicPartition(entry.getKey(),i),new ArrayList<>());
            }
        }
//        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
////
////        }
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            entry.getValue().topics().stream().filter(topic->partitionsPerTopic.get(topic)!=null).forEach(
                topic->{
                    for (Integer i = 0; i < partitionsPerTopic.get(topic); i++) {
                        TopicPartition topicPartition = new TopicPartition(topic, i);
                        consumerForPartition.get(topicPartition).add(entry.getKey());
                    }
                }
            );
        }
        return consumerForPartition;
    }
}

```



#### 自定义组内广播策略

在kafka的默认消费逻辑中，在同一个消费组中，不能有多个消费者消费同一个分区。而自定义分配方案便可打破这个规定。下面是一个自定义实现组内广播的分配方案。实现了同一个消费者组，所有消费者消费相同的分区。

```java
public class GroupBroadcast extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {

        Map<String, List<TopicPartition>> assigment = new HashMap<>();
        //获取所有消费者
        Set<String> consumers = subscriptions.keySet();
        for (String consumer : consumers) {
            assigment.put(consumer,new ArrayList<TopicPartition>());
        }
        //获取所有的topic
        Set<String> topics = partitionsPerTopic.keySet();

        //配置topic与consumer的对应关系
        Map<String, List<String>> consumerForTopic = getConsumerForTopic(partitionsPerTopic, subscriptions);

        //构建每个topic对应分区集
        HashMap<String, List<TopicPartition>> partitionForTopic = new HashMap<>();
        //把每一个topic的分区分给订阅该topic的所有消费者
        for (String topic : topics) {
            List<String> consumerList = consumerForTopic.get(topic);
            //构建每个topic对应分区集
            for (Integer i = 0; i < partitionsPerTopic.get(topic); i++) {
                List<TopicPartition> partitions = partitionForTopic.computeIfAbsent(topic, k -> new ArrayList<TopicPartition>());
                partitions.add(new TopicPartition(topic,i));
                partitionForTopic.put(topic,partitions);
            }

            for (String consumer : consumerList) {
                assigment.get(consumer).addAll(partitionForTopic.get(topic));
            }
        }


        return assigment;
    }

    @Override
    public String name() {
        return "GroupBroadcast";
    }

    public Map<String,List<String>> getConsumerForTopic(Map<String, Integer> partitionsPerTopic,
                                                        Map<String, Subscription> subscriptions){
        Set<String> topics = partitionsPerTopic.keySet();
        Map<String, List<String>> consumerForTopic = new HashMap<>();

        for (String topic : topics) {
            consumerForTopic.put(topic,new ArrayList<String>());
        }

        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            for (String topic : entry.getValue().topics()) {
                consumerForTopic.get(topic).add(entry.getKey());
            }
        }
        return consumerForTopic;
    }
}

```

