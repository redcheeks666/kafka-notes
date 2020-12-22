# kafka的消息传递语义

* [kafka的消息传递语义](#kafka的消息传递语义)
  * [幂等](#幂等)
    * [幂等的限制](#幂等的限制)
    * [开启幂等](#开启幂等)
      * [max\.in\.flight\.requests\.per\.connection](#maxinflightrequestsperconnection)
    * [幂等原理](#幂等原理)
      * [PID](#pid)
      * [sequenceNumber](#sequencenumber)
      * [幂等开启后的消息发送流程](#幂等开启后的消息发送流程)
      * [幂等开启后服务端操作流程](#幂等开启后服务端操作流程)
  * [事务](#事务)
      * [Kafka事务解决了哪些事？](#kafka事务解决了哪些事)
      * [Kafka对于事务提出的概念](#kafka对于事务提出的概念)
      * [事务流程中数据安置问题](#事务流程中数据安置问题)
        * [Server服务端](#server服务端)
        * [Consumer客户端](#consumer客户端)
      * [kafka事务整体流程](#kafka事务整体流程)
        * [1\.寻找Producer对应的事务协调器](#1寻找producer对应的事务协调器)
        * [2\.初始化PID](#2初始化pid)
        * [3\.开始事务](#3开始事务)
        * [4\.Commer\-Producer loop](#4commer-producer-loop)



一般而言，对于消息传递的语义保证，有三种模式。

- At most once—最多发送一次，消息可能丢失，但绝不会重复。
- At least once—至少发送一次，消息可能会重复，但绝不会丢失。
- Exactly once—精准发送一次，只传递一次。

对于以上三个模式，我们可以从两方面来理解，生产者消息的持久性和消费者端使用消息时的持久性保证。

许多系统声称提供“完全一次”的消息传递语义，但大多数是误导性的。他们不会去解释一些情况下出现的问题，比

如，消费者或生产者可能失败的情况，或者有多个消费者时情况，再或者写入磁盘数据也可能丢失的情况。



kafka的语义是直接了当的。当发布消息时，我们有一个消息被“提交到”日志的概念，即一旦消息被提交，只要写

入的分区和所在Broker保持活跃，由于多副本的机制，此消息就不会丢失。如果生产者在发布消息时出现网络错

误问题，无法断定该错误是在提交消息之前发生还是之后发生，这类问题类似于使用自动生成Key插入数据库表的

语义。



## 幂等

幂等，即多次调用接口产生的结果与调用一次接口的结果一致。避免了生产者在进行重试发送时产生的消息重复。

在0.11.0.0版本之前，kafka生产者在未收到已提交的响应时，除了重新发送消息别无选择。也就是在0.11.0.0版本

之前，这提供了至少一次(At least once)的消息传递语义。从0.11.0.0版本开始，KafkaProducer提供了幂等的发

送机制，该机制保证了重发不会导致日志的重复。为此，Broker在初始化Producer时，会给其分配一个

ProducerId，随每个消息发送的，还有一个sequence number(序列号)，这样，使用

PID+Message+SequenceNumber对每个消息的重复数据进行删除，达到幂等。

### 幂等的限制

- 幂等只能保证Producer在单个回话中保证消息不丢失，不重复。也就是说，当Producer出现故障重启，是无法保证幂等的
- 幂等不能跨多个Topic-partition,其中sequenceNumber是针对单个分区的。中间状态并未同步，所以无法保证多分区的幂等。

### 开启幂等

开启幂等功能：

```java
//是否开启幂等(默认false)
properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
```

如果用户没有配置重试次数，则kafka默认配置为，如果配置了，则重试次数必须大于0.

```java
if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
```

如果配置了max.in.flight.requests.per.connection(连接最大缓存请求数)，最大配置为5，默认为5.

#### max.in.flight.requests.per.connection

> max.in.flight.requests.per.connection
>
> 此参数设置默认为5，单个连接下，能够发送未响应请求的个数。也就是说，此参数为1时，在只有收到服务端响应的情况下才能发送下一Batch请求。
>
> 此外，服务端的`ProducerStateManager`会缓存每个PID在每个分区上最近的5个Batch数据，这个5是定义在服务端的常量。超过5时，服务端会将最旧的的Batch删除。
>
> 假如，max.in.flight.requests.per.connection参数设置为6，此时，此时6个请求都发送了出去，服务端只缓存了2-6的Batch，第一个Batch被删除了。如果此时第一个请求发送失败了，服务端Log对象`analyzeAndValidateProducerState()`方法会将Batch与`ProducerStateManager`中缓存的5个Batch通过sequenceNember进行比对是否存在，判断为否。由于开启了幂等，接下来就需要`checkSequence()`校验sequenceNumber的值，此时，sequenceNember是无法保持连续的，而客户端会校验，非连续时则抛出`OutOfOrderSequenceException`异常。客户端收到此异常会继续重试，直到超时或超最大重试次数。



**MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION 大于1如何保证消息的有序性？**

**未开启幂等（参数值大于1）**

例如，此参数配置为6，客户端此时发送了6个请求(1-6)，其中，2-6成功了。请求1失败了，此时需要重试，即将请求1的Batch放入待发送队列中，再次发送时，数据即出现乱序。

**未开启幂等（参数值等于1）**

再例如，此参数设置为1，只有当每次请求得到服务端响应时，才可以发送下一次请求，类似单线程处理，在一个客户端情况下虽然可以解决乱序问题，但单线程模式过于效率低。

**开启幂等（参数值大于1小于5）**

在参数大于1小于5时，当请求出现重试时，Batch会被添加到队列中，按照sequenceNmber大小顺序，添加到队列的合适位置。`reenqueue()`方法.

Sender线程在发送请求时，通过消息累加器RecordAccumulator的drain()方法获取数据，遍历队列中的Batch，通过Batch中的sequenceNumber是否有值来判断是否是重试的Batch，非重试的Batch此时是没有序列值的。如果是重试的Batch，客户端会等待此队列的请求完成以后才会继续向该分区发送消息。这是因为当服务端处理producerequest请求时，会要求此Batch的序列值时连续的，如果连续则全部返回异常，客户端收到异常后重试。

也就是说，例如，客户端发送了5个请求（1-5），此时2请求失败了，请求3，4，5在服务端进行比对时，序列值是不连续的，此时2，3，4，5都会被添加到队列中，再次重试。

此机制步骤可概括为：

- 服务端验证Batch的序列值，不连续时进行返回异常。
- 重试的Batch会按照序列值进行排序。存入合适的位置。
- Sender线程进行重试发送时，需要等待当前序列中未得到响应的请求返回结果后继续发送。

如果参数max.in.flight.requests.per.connection大于5则会抛异常。

```java
private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }
```

如果配置了acks的值，必须为-1，未配置，kafka会默认将其置为-1.

```java
if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG + " to all in order to use the idempotent " +
                    "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
```



### 幂等原理

#### PID

每一个Producer的唯一标识，Kafka可能存在多个Producer,但我们只需要保证每个生产者内部的消息保持幂等即可，所以，使用PID来进行Producer区分。PID是全局唯一的，是生产者每次初始化时，服务端为其分配的，每次重启，PID都会进行重新分配不同的值。



**1.PID的获取**

在客户端的Sender线程中，Kafka的Sender线程没有PID时会进行初始化InitProducerIdRequest请求，服务端收到此请求后，KafkaApis匹配到handleInitProducerIdRequest，其中，实际调用了TransactionCoordinator的handleInitProducerId()请求 。

```scala
    //TransactionCoordinator>handleInitProducerId
	if (transactionalId == null) {
	//如果transactionalId为空,说明只开启了幂等。取到PID直接返回即可。
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    }
```

**2.PID的管理**

- ProducerIdManager初始化时或ID段使用溢出时，会向ZK申请新的ID段，每次1000个。
- zk 中有一个 `/latest_producer_id_block` 节点，每个 Broker 向 zk 申请一个 PID 段后，都会把自己申请的 PID 段信息写入到这个节点(包括startId和endId)。提供给其他Broker再次申请。

```scala
//ProducerIdManager
def generateProducerId(): Long = {
    this synchronized {
      // 返回增1nextProducerId值,如果ID段已使用溢出,则重新申请ID段。
      if (nextProducerId > currentProducerIdBlock.blockEndId) {
        getNewProducerIdBlock()
        nextProducerId = currentProducerIdBlock.blockStartId + 1
      } else {
        nextProducerId += 1
      }

      nextProducerId - 1
    }
  }
```



#### sequenceNumber

对要写入的分区，维护一个序列sequenceNumber，从0开始递增。



**sequenceNumber校验规则**

sequenceNumber随消息一起发送，Broker端在内存中保存了此序列的当前值，当接收到新发送的消息，如果是

同一分区的消息中序列号比Broker中保存的序列号刚好大1，则接收，否则丢弃。这样，即实现了避免消息重复提

交，但这只能保证单个分区内消息幂等，不能保证不同分区的消息幂等。



#### 幂等开启后的消息发送流程

1.Kafkaproducer通过`Send()`方法将数据缓存至ProducerBatch中。

2.后台发送线程Sender的`run()`方法中，首先会判断PID是否需要重置，重置的原因是当Batch中的数据重试发送多次，因超时失败，此时便需重置。因为序列值已被分配出去，消息中的sequenceNumber无法做到连续。TransactionManager 遇到此种问 题是无法保证精准一次的语义的。(属于严重错误)。

3.Sender线程的run()方法中，如果需要，会调用maybeWaitForProducerId()申请获取PID和版本epoch信息。

4.最后，Sender线程sendProduceRequests()方法，发送ProduceRequest请求至服务端。



#### 幂等开启后服务端操作流程

1.校验相关权限

2.LOG对象会在`analyzeAndValidateProducerState()`方法中根据Batch的序列值判断是否存在缓存中的Batch中，将存在的Batch请求返回给客户端。

3.更新Producer信息。

4.正常append。



**总的来讲，幂等即用PID区分了Client,在区分了Client的情况下，通过sequenceNumber来进行区分消息的唯一。**



## 事务

同样是在kafka 0.11.0.0版本中，增加了事务的功能。解决了部分幂等未解决的问题。



#### Kafka事务解决了哪些事？

1. 原子性：跨会话，跨分区的多批次消息原子性写入。
2. 耐用性：保障消息不丢失。
3. 唯一性：不会出现重复消息。
4. 交织性：每个分区都可以接收来自事务生产者和非事务生产者的消息
5. 顺序性：事务性与非事务性消息都会按照消息源顺序排序

#### Kafka对于事务提出的概念

**事务协调器(TransactionCoordinator)**：与消费者协调器(GroupCoordinator)类似，每个生产者对应一个事务协调器,用于分配PID和管理事务。

**内部事务Topic(__transaction_state)**：持久化事务协调器状态,事务协调器做故障恢复时从中取数据。

**事务结束标记(EndTransactionMarker)**：此标记标示着一个事务的结束,包括commit或abort,由TransactionCoordinator发送。

**事务ID(TransactionalId)**：用户提供,每个生产者唯一。相同的TransactionalId不同生产者实例可以恢复(或终止)由之前实例开启的任何事务。

**生产者纪元(ProducerEpoch)**：确保给定的事务Id的生产者只能有一个合法活跃的实例，使得发生故障维护事务提供保障。



#### 事务流程中数据安置问题

对于处于事务中的数据，如果存入缓存，待分区Leader收到commit或者abort的EndTransactionMarker消息时再

做处理,肯定是不理想的,如果事务流程较长，数据量大，内存无法支撑，对此，kafka的做法是直接将数据存入磁

盘。

##### Server服务端

在消息结构中，有一个attributes的字段，其中，第5位标识当前消息是否是事务消息，如果是，则为1，否则

为0。第6位标识当前消息是否为控制消息(即 EndTransactionMarker消息)，如果是则为1，否则为0。在事务结

束时，TransactionCoordinatior会向各个分区Leader发送请求，对应分区收到请求写入控制消息。以此标识事务

的结束。

Control Batch只有一条消息，Key中保存控制类型，0代表abort,1代表commit。value保存

TransacationCoordinator的版本。

##### Consumer客户端

Kafka提出了一个LSO的概念，即last stable offset，对于分区而言，offset小于LSO的消息都为稳定的消息，即事

务状态已经确定的消息，要么提交过的，要么回滚过的。Broker对于read_committed的Consumer，只提供

offset小于LSO的消息。

在server返回给Consumer数据时，除了Batch,还会返回一个abortedTransactions，abortedTransactions是一个队列，其中保存的是事务的每个PID对应事务的起始偏移量，即`<PID,firstOffset>`。

每次Consumer拉取到数据，会进行以下一个操作：

```java
private Record nextFetchedRecord() {
            while (true) {
                if (records == null || !records.hasNext()) {
                    maybeCloseRecordStream();

                    if (!batches.hasNext()) {
                        // Message format v2 preserves the last offset in a batch even if the last record is removed
                        // through compaction. By using the next offset computed from the last offset in the batch,
                        // we ensure that the offset of the next fetch will point to the next batch, which avoids
                        // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                        // fetching the same batch repeatedly).
                        if (currentBatch != null)
                            nextFetchOffset = currentBatch.nextOffset();
                        drain();
                        return null;
                    }

                    currentBatch = batches.next();
                    lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                            Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());

                    maybeEnsureValid(currentBatch);

                    if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                        // remove from the aborted transaction queue all aborted transactions which have begun
                        // before the current batch's last offset and add the associated producerIds to the
                        // aborted producer set
                        //查看本批次的消息，最后一个偏移量是否大于等于abortedTransactions队列中第一个事务的firstOffset(即是否开启了新的事务)
                        //大于等于:开启了新的事务,将队列中首个事务删除,并将开启事务对应的生产者PID加入abortedProducerIds。
                        //小于:消息还处于上一次的事务中。
                        consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                        long producerId = currentBatch.producerId();
                        //true:如果是控制消息，且控制消息类型为abort
                        //false:非控制消息(普通消息)
                        if (containsAbortMarker(currentBatch)) {
                            //此批次为控制消息，且为终止类型，表示此事务已完毕。PID集中移出此批次对应的PID
                            abortedProducerIds.remove(producerId);


                            //如果消息为普通消息，且消息为终止事务中的消息，则跳过。
                            //判断逻辑为，如果消息的PID存在于abortedProducerIds，则为终止事务消息。
                        } else if (isBatchAborted(currentBatch)) {
                            log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                          "offsets {} to {}",
                                      partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                            nextFetchOffset = currentBatch.nextOffset();
                            continue;
                        }
                    }
                    //生成消息集。此迭代器中，包括已提交的普通消息，和Commit类型的控制消息。
                    records = currentBatch.streamingIterator(decompressionBufferSupplier);

                } else {
                    //循环取出消息实例
                    Record record = records.next();
                    // skip any records out of range
                    //跳过nextFetchOffset范围的消息。(nextFetchOffset通过此标识来控制偏移量)
                    if (record.offset() >= nextFetchOffset) {
                        // we only do validation when the message should not be skipped.
                        maybeEnsureValid(record);

                        // control records are not returned to the user
                        //校验是否是普通消息
                        if (!currentBatch.isControlBatch()) {
                            //普通消息返回给用户
                            return record;
                        } else {
                            //控制消息跳过。
                            // Increment the next fetch offset when we skip a control batch.
                            nextFetchOffset = record.offset() + 1;
                        }
                    }
                }
            }
        }
```



#### kafka事务整体流程

![Kafka 事务流程](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\Kafka 事务流程.png)



> 图片摘自官网



##### 1.寻找Producer对应的事务协调器

生产者开启事务第一件事便是寻找所对应的事务协调器.
**客户端**:发送FindCoordinatorRequest，其CoordinatorType为TRANSACTION。
**服务端**:收到FindCoordinatorRequest请求后,通过TransactionalId的Hash值与`__transaction_state`分区总数(默认50)取模后的值所对应的`__transaction_state`分区Leader即为该Producer的事务协调器。

```scala
//寻找事务协调器算法
def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount
```



##### 2.初始化PID

找到事务协调器后，下一步就是检索PID，通过向事务协调器发送InitPidRequest实现。

 	**2.1指定了TransactionalId时(即开启事务时)，InitPidRequest进行以下操作**

 	2.11把PID持久到事务日志中，为了以后能把PID返回给相同的TransactionalId生产者实例，从而恢复或终止先前未完成的事务。

 	2.12递增PID的纪元(版本)，让历史的生产者实例受限无法继续进行事务。

 	2.13恢复生产者实例之前留下的未完成事务实例

InitPidRequest处理是同步的，只有返回后才可发送数据，并开始新的事务。

 	**2.2未指定TransactionalId时**

分配新的PID，并在单个会话享受幂等和事务语义。



##### 3.开始事务

kafka提供了一个beginTransaction()方法开启新的事务，生产者记录事务开始的本地状态，直到发送第一条记录，事务才从TransactionCoordinator角度开始。



##### 4.Commer-Producer loop

**4.1AddPartitionsToTxnRequest**

如果生产者写入一个新的分区作为事务的一部分，会发送`AddPartitionsToTxnRequest`请求，TransactionCoordinator会将分区信息存储在`__transaction_state`中，以便后续将Commit或Abort信息写入分区。

**4.2ProduceRequest**

将消息与PID,Epoch,Sequence写入分区。

**4.3 AddOffsetCommitsToTxnRequest**

sendOffsetsToTransaction方法可批处理消息消费和发送，该sendOffsetsToTransaction方法发送一个AddOffsetCommitsToTxnRequests与的groupId到事务协调器，通过GroupId找到对应`__consumer_offset`对应的分区，TransactionCoordinator将此分区保存在__transaction_state中。

**4.4 TxnOffsetCommitRequest**

同样作为sendOffsets的一部分，生产者将发送TxnOffsetCommitRequest到TransactionCoordinator，将事务包含的偏移量保存在`__consumer_offsets`中。

**5 Committing or Aborting a Transaction**

提交或终止事务，写入数据后，调用commitTransaction或abortTransaction方法，提交/终止事务。



5.1 commitTransaction和abortTransaction都会向TransactionCoordinator发送EndTxnRequest请求标识

Commit或Abort，TransactionCoordinator在收到请求后会做以下操作。

1.将事务mata信息状态更新为PREPARE_COMMIT或者PREPARE_ABORT，并持久化到`__transaction_state`中。

2.根据事务mata状态信息，向事务涉及的分区Leader发送TransactionMarker控制消息。(WriteTxnMarkersRequest请求)

3.将事务mata状态更改为COMMIT或ABORT，并将事务的mata持久化到`__transaction_state`。



5.2 WriteTxnMarkersRequest 

对应节点收到此请求，向分区写入控制消息，控制消息的RecordBatch中attributes字段第5位为1(0表示非事务消息)，第6位为1(0表示普通消息)。

控制消息成功写入后，TransactionCoordinator将Complete_Commit和Complete_Abort写入`__transaction_state`，此时，TransactionCoordinator缓存的关于此事务的数据都可被清除。

`__transaction_state`日志清除策略默认为日志压缩(同一个key，保留最新数据)。



至此，事务整体流程结束。