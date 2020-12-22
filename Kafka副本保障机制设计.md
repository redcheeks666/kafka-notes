# kafka副本保障机制设计思想

> 此文章内容参考总结自官网。
>
> http://kafka.apache.org/23/documentation.html#replication

* [kafka副本保障机制设计思想](#kafka副本保障机制设计思想)
  * [kafka对于副本提出的概念](#kafka对于副本提出的概念)
  * [MQ友商副本缺点（官网所述，非个人观点）](#mq友商副本缺点官网所述非个人观点)
  * [kafka副本设计](#kafka副本设计)
      * [副本活跃节点的精准定义](#副本活跃节点的精准定义)
  * [Kafka对于副本的保证](#kafka对于副本的保证)
  * [副本日志：Quorums，ISRs，and State Machines (Oh my\!)](#副本日志quorumsisrsand-state-machines-oh-my)
    * [State Machines](#state-machines)
    * [Quorums](#quorums)
    * [少数服从多数算法](#少数服从多数算法)
    * [Kafka副本机制算法（ISR）](#kafka副本机制算法isr)
    * [关于移入移出ISR副本集](#关于移入移出isr副本集)
  * [Unclean leader election: What if they all die?](#unclean-leader-election-what-if-they-all-die)
  * [可用性和持久性保证(acks)](#可用性和持久性保证acks)
    * [数据可用性acks](#数据可用性acks)
    * [数据持久性](#数据持久性)

    ​

## kafka对于副本提出的概念

1.kafka的副本是服务端为每一个Topic分配的,是可配置数量的。(可以针对每一个topic配置副本因子)。其目的是为

了在服务端某一台节点出现故障时,能够自动实现故障转移，从而在出现故障时，保持消息任然可用。

2.分区的所有的副本集称为AR，能够跟上Leader节奏的副本集称为ISR(包括Leader本身也属于此副本集)，非同步副本称为OSR。

3.每个分区最后一条消息的下一个位置称为LEO，分区副本的LEO可能都不相同。ISR副本集中，最小的LEO称为HW，即水位线。普通的消费者拉取消息时，只能拉取到HW之前的消息。



## MQ友商副本缺点（官网所述，非个人观点）

其他消息系统也提供了一些与副本有关的特性，但在我们看来，似乎是附加的东西，并没有被频繁的使用到。并且通常有一个很大的缺点，当副本处于非活跃状态。吞吐量会因此受到严重的限制，它需要频繁精准的手动配置，kafka默认是和副本一起被使用的，事实上，我们实现了将未配置副本的topic实现为副本因子为1的topic。**副本是针对kafka-topic-partitions分区来讲的，在非故障情况下,kafka的每个分区都有一个Leader和0-N个follower,包括Leader在内,这些统称为副本因子。所有的读写操作都对接Leader.**通常,partitions要比Broker多很多，并且Leader均匀的分布在不同的Broker上，理想状况下，Follower与Leader的步调保持一致。-都具有相同的顺序和偏移量的消息。(当然，在给定的特殊时间,Leader日志的末尾，可能存在Follower尚未复制的消息。)



## kafka副本设计

Follower从Leader拉取消息同步在自己的日志中，此操作与普通的消费者拉取消费消息一致。Follower从Leader拉取消息有一个很好的特性，就是他们可以很自然的把日志条目分批次的
写入自己的日志文件。

#### 副本活跃节点的精准定义

与大多数分布式系统一样，自动处理故障需要对节点的"活跃状态"进行精准定义，kafka中，对于活跃节点有两个要求：

1.节点和Zookeeper保持心跳

2.如果是Follower，必须能跟上Leader的节奏。(复制Leader的日志，并不过于落后于Leader).

满足以上两个条件的节点我们称之为'同步节点'，避免混淆'有效节点'和'故障节点',**Leader维护了一个集合,集合里面的节点都是能够跟上Leader节奏的副本,(ISR副本集).**如果Follower宕机,卡顿,或者没跟上Leader的节奏,Leader将此副本从ISR副本集中删除.



## Kafka对于副本的保证

**Kafka提供的保证是，只要始终有至少一个活着的同步副本，提交的消息就不会丢失。**



## 副本日志：Quorums，ISRs，and State Machines (Oh my!)

### State Machines

kafka分区的本质是一个副本的日志。副本日志是分布式数据系统中最基本的要素之一，有很多方法可以实现它。其他系统也可以用 kafka 的备份日志模块来实现状态机风格的分布式系统



副本日志通过一定的顺序进行对日志的编号，排序。有很多方法可以实现此排序，最简单最直接的方式就是由Leader节点提供副本所需要的顺序，只要Follower还是活跃的，那么所有Follower拷贝数据只需要按Leader顺序进行排序即可。



### Quorums

如果Leader宕机了，那么我们就需要选举一个新的Leader,并且要确保新的Leader拥有着最多的日志，也就是数据同步最新的节点。这之间就存在着一种权衡，Leader收到一条消息后，如果需要等待更多的Follower进行确认同步，然后进行响应，那么当选举新Leader时，就会有更多的候选人。



写入时，必须保证一定量副本写入成功，确保写入消息存在于多副本，那么此算法称为Quorums。



### 少数服从多数算法

这是一种常见的消息提交算法，但这并不是Kafka所使用的，我们可以探讨下此算法。

假如有`2f+1`个副本，当其中`f+1`个副本收到消息并返回响应给Leader时，等到选举新Leader时，候选人至少有`f+1`个，如此，可以容忍` f `个副本故障。当`f`个副本故障时，至少有一个有着完整的日志。可以被选作Leader.

每个算法都要处理更多的细节，比如精准定义如何保持副本完整，确保Leader宕机时，能够保证日志一致性。这些，我们在此暂时忽略。



优点：

延迟取决于最快的服务器。取决于最快的`f+1`个节点。

缺点：

对于副本总量相同的情况下，容忍副本故障数量较少。 反之，如果需要容忍1个副本故障，则需要三个副本，容忍故障两个副本需要五个副本总量。此种方式对磁盘需求量大，吞吐量低，处理海量数据是不切实际的。所以Quorum算法更多用共享集群配置，如Zookeeper，不适用于原始数据存储，比如HDFS中NameNode高可用采用的是少数服从多数，但数据存储并不是此算法，高昂的存储方式不适用于数据本身。



此系列有许多算法，包括Zookeeper的Zab,Raft, 和 Viewstamped Replication。与Kafka算法相似的是微软的PacificA。



### Kafka副本机制算法（ISR）

kafka采取了一种略微不同的方式，即动态的维护了一个ISR(a set of in-sync replicas)，这个集合中的Follower节点与Leader日志保持高度一致，只有处于此副本的成员才有资格选举为Leader，一条消息必须被此集合中所有节点都同步后才可以被视为提交。

对于kafka的这种ISR算法，如果有`f+1`个副本，可以容忍f个副本故障。

对比少数服从多数算法，同样是容忍f个副本故障，少数服从多数算法需要提供2f+1个副本。



少数服从多数算法的一个特点就是可以避开最慢的服务器，但是，kafka的ISR可以允许客户端选择是否阻塞消息来改善，对比高吞吐和低磁盘占用，这是值得的。



另外，kafka并不要求节点宕机恢复时，所有的数据保持完整，这种要求依赖于存储的稳定，在保证出现故障不会丢失。其中有两个问题，第一磁盘错误时，通常不能保证数据完整性。其次，即使不是磁盘错误，我们也不希望通过同步来保证一致性，因为这会使性能降低两到三个数量级。我们确保副本在重新加入ISR之前，即使在它宕机到恢复没有新的数据产生，他也必须完成一次数据同步。



### 关于移入移出ISR副本集

**移出**

> 在一定时间内，没有跟上Leader的副本会被移出ISR集，此时间由replica.lag.time.max.ms配置。
>
> 在Follower追上Leader时，会更新一个lastCaughtUpTimeMs标识，此标识存的是最近一次同步上LeaderLEO之前的日志的时间，Kafka通过定期检测该标识与当前的时间差是否大于配置的replica.lag.time.max.ms。

```shell
#ISR移出系数
replica.lag.time.max.ms#默认10000
#0.9版本之前的另一个参数，后续版本已移除。
#replica.lag.max.messages
```

**移入**

> 随着时间流逝，Follower会一直拉取Leader的数据并同步自己的日志，当自身日志最后一条LEO大于Leader的HW时，Follower会重新加入到ISR集合。



> 当ISR副本有变更时，会将变更后的记录记录在Zookeeper中。

```shell
#Zookeeper中的对应节点
#/brokers/topics/＜topic＞/partition/＜parititon＞/state
```



## Unclean leader election: What if they all die?

所有节点都挂了，怎么实现数据保障？

kafka提供了两种策略：

1.等一个ISR集中的副本恢复正常，选他作为Leader.

2.选第一个恢复的副本，不一定是ISR集合中的，并选他作为Leader.

这是一致性与可用性的权衡，kafka默认使用的是第一种，可以通过配置改为第二种策略。

```shell
#默认为false 即上述第一种策略。true则为第一种
unclean.leader.election.enable
```

这种困境并不是Kafka独有，存在于所有的quorum-based算法。如果大多数副本节点都宕机了，那么要么选择100%丢失数据，要么从剩下的选一个。



## 可用性和持久性保证(acks)

### 数据可用性acks

写数据时，Producer可通过配置acks来调节等待副本同步数量。

acks=0 : 不等待Leader确认提交结果。

acks=1：Leader返回成功即返回

acks=-1/all : ISR集合中所有副本都同步完成返回。

> acks=-1/all并不能保证所有副本都同步到消息，如果ISR集合中只剩下Leader副本，那么效果与acks=1一致。

### 数据持久性

对于数据持久性，kafka针对topic提出了两个配置策略。

1.unclean.leader.election.enable参数配置为false，所有副本宕机时，等待最先恢复的副本选举为Leader，尽可能避免数据的丢失。

2.指定最小的ISR集合，当ISR集合满足最小值，才可进行写入操作。这个设置只有在生产者使用 acks = all 的情况下才会生效。这降低了可用性，是一致性与可用性之间的折中。































