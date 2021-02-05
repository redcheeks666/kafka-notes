# kafka消费者再均衡

## 什么是消费者再均衡？

> 个人理解，消费者再均衡就是消费者与分区的关系发生改变，分区的消费者由一个消费者变成另一个消费者。

## 什么时候会发生再均衡

> 1. 新的消费者加入消费者组
> 2. 消费者退出消费者组，如取消订阅某些topic
> 3. 消费者宕机
> 4. topic分区数发生改变

在kafkaConsumer的subscribe方法中，有一个监听器ConsumerRebalanceListener listener，此回调接口有俩方法:

此方法在再均衡之前，消费者停止读取消息之后调用，可以用来在再均衡前提交位移，避免再均衡后重复消费，参数为再均衡之前的分区分配。

> ```
> void onPartitionsRevoked(Collection<TopicPartition> partitions);
>
>
> ```

另外一个，在分区重分配，消费者重新读取消息之前调用，参数为再均衡后的分区分配

> ```
> void onPartitionsAssigned(Collection<TopicPartition> partitions);
>
>
> ```

我们先来用这个回调直观的看一下再均衡的发生:

> 首先，我们准备三个消费者。订阅相同的topic ,该topic有四个分区，三个消费者在同一个消费者组，三个消费者分别为client-1,client-2,client-3,subscribe()方法代码如下

```
kafkaConsumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.print("再均衡之前该消费者分配的分区");
        for (TopicPartition partition : partitions) {
            System.out.printf("  %s",partition.partition());
        }
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.print("再均衡之前该消费者分配的分区");
        if (partitions != null) {
            for (TopicPartition partition : partitions) {
                System.out.printf("  %s",partition.partition());
            }
        }
    }
});


```

首先我们启动第一个消费者client-1,

client-1打印日志为

```
再均衡之前该消费者分配的分区  3  0  2  1


```

接着我们启动第二个client-2

client-2日志为

```
再均衡之后该消费者分配的分区  0  1


```

此时触发再均衡，client-1控制台打印出

```
再均衡之前该消费者分配的分区  3  0  2  1
再均衡之后该消费者分配的分区  3  2


```

我们再来启动第三个client-3

```
再均衡之前该消费者分配的分区  0  1


```

此时client-1的日志为

```
再均衡之前该消费者分配的分区  3  2
再均衡之后该消费者分配的分区  3


```

client-2日志为

```
再均衡之前该消费者分配的分区  0  1
再均衡之后该消费者分配的分区  2


```

从消费者对应分区宏观上变化,很明显的可以看出，Kafka的再均衡的触发。那么在这分区副本变化的过程中发生了什么？

## kafka的再均衡流程图

![img](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/kafka%E5%86%8D%E5%9D%87%E8%A1%A1%E6%B5%81%E7%A8%8B%E5%9B%BE.png)

## kafka再均衡原理

**第一阶段**

1.消费者客户端判断是否保存了GroupCoordinator信息，且网络畅通，则直接进入第二阶段，否则，向broker集群中负载最小的节点发送FindGroupCoodinator请求，寻找GroupCoordinator节点信息

2.broker集群收到请求后，根据请求中的groupId计算出该groupId所对应的__consumer_offset分区号，

该分区的Leader节点即此groupId对应的GroupCoordinator节点.具体算法为：

```
//（groupMatadataTopicPartitionCount默认50）
Util.abs(groupId.hashCode()%groupMatadataTopicPartitionCount)


```

3.找到消费者组协调器的节点后，消费者协调器ConsumerCoordinator向服务端的GroupCoordinator节点发送JoinGroupRequest请求。

4.GroupCoordinator收到JoinGroupRequest请求后，校验合法性后选举消费者Leader和分区分配方案。后响应给消费者客户端

GroupCoordinator接收组内消费者的分配方案，和消费位移。

**第二阶段**

如果是原有的消费者重新加入消费组，那么在真正发送JoinGroupRequest 请求之前还要执行一些准备工作：

（1）如果消费端参数enable.auto.commit设置为true（默认值也为true），即开启自动提交位移功能，那么在请求加入消费组之前需要向 GroupCoordinator 提交消费位移。这个过程是阻塞执行的，要么成功提交消费位移，要么超时。（2）如果消费者添加了自定义的再均衡监听器（ConsumerRebalanceListener），那么此时会调用onPartitionsRevoked（）方法在重新加入消费组之前实施自定义的规则逻辑，比如清除一些状态，或者提交消费位移等。（3）因为是重新加入消费组，之前与GroupCoordinator节点之间的心跳检测也就不需要了，所以在成功地重新加入消费组之前需要禁止心跳检测的运作。

**选举消费者组Leader** ：如果没有Leader,第一个加入的消费者就是Leader，如果Leader退出消费者组，则随机选出一个。

**选举分区分配策略** ：每个消费者都有权配置分配方案，GroupCoordinator会收集各个消费者分配方案，票数最多的为当前消费者组分配方案。

当消费者不支持此方案时，会抛出异常，IllegalArgumentException：Member does not support protocol。所支持策略即当前消费者客户端参数partition.assignment.strategy 所配置策略。

之后，将分配策略响应给客户端

**第三阶段**

1.消费者组Leader拿到分配信息后进行具体分区分配，后各个消费者带着消费者元数据向GroupCoordinator节点发送同步消费者组请求SyncGroupRequest,Leader消费者除元数据外还会携带具体分区分配的方案。

2.GroupCoordinator在收到SyncGroupRequest请求后，会把分配方案和元数据存入Kafka内部的__consumer_offset中。然后把分配分区的具体方案响应给各个消费者

3.消费者拿到分配方案后，会调用再平衡监听器中的方法，然后发送心跳到GroupCoordinator确定彼此在线

**第四阶段**

发送offsetFetchRequest请求到GroupCoordinator获取之前保存的位移。

定期向GroupCoordinator发送心跳
