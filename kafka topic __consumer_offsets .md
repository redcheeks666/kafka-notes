# KAFKA(__consumer_offsets)

* [KAFKA(\_\_consumer\_offsets)](#kafka__consumer_offsets)
  * [创建\_\_consumer\_offsets的源码:](#创建__consumer_offsets的源码)
  * [\_\_consumer\_offsets的创建时机](#__consumer_offsets的创建时机)
  * [KafkaConsumer\.java](#kafkaconsumerjava)
  * [ConsumerCoordinator\.java](#consumercoordinatorjava)
  * [提交位移请求响应结构](#提交位移请求响应结构)
    * [客户端提交位移发送OffsetCommitRequest请求](#客户端提交位移发送offsetcommitrequest请求)
    * [Offset Commit Response响应](#offset-commit-response响应)
  * [查看指定消费者组对应\_\_consumer\_offsets分区的内容:](#查看指定消费者组对应__consumer_offsets分区的内容)



**由于Zookeeper并不适合大批量的频繁写入操作，新版Kafka已推荐将consumer的位移信息保存在Kafka内部的topic中，即__consumer_offsets topic，并且默认提供了kafka_consumer_groups.sh脚本供用户查看consumer信息。**

## 创建__consumer_offsets的源码:

> KafkaApis.scala

```scala
    topic match {
      case GROUP_METADATA_TOPIC_NAME =>
        if (aliveBrokers.size < config.offsetsTopicReplicationFactor) {
          error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
            s"'${config.offsetsTopicReplicationFactor}' for the offsets topic (configured via " +
            s"'${KafkaConfig.OffsetsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
            s"and not all brokers are up yet.")
          new MetadataResponse.TopicMetadata(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, java.util.Collections.emptyList())
        } else {
          createTopic(topic, config.offsetsTopicPartitions, config.offsetsTopicReplicationFactor.toInt,
            groupCoordinator.offsetsTopicConfigs)
        }
```

```scala
createTopic(topic, config.offsetsTopicPartitions, config.offsetsTopicReplicationFactor.toInt,
            groupCoordinator.offsetsTopicConfigs)
```

两个参数partitions分区数,ReplicationFactor副本数：

config.offsetsTopicPartitions(副本)

> KafkaConfig.scala

```java
val TransactionsTopicPartitionsProp = "transaction.state.log.num.partitions"
```

config.offsetsTopicReplicationFactor.toInt(副本)

> KafkaConfig.scala

```java
val OffsetsTopicReplicationFactorProp = "offsets.topic.replication.factor"
```

可以看到，内部主题__consumer_offsets的分区数和副本数取值是可配置的，即上面两个参数。默认的情况下，transaction.state.log.num.partitions的值为50，offsets.topic.replication.factor为3。

## __consumer_offsets的创建时机

在KafkaApis.scala中 kafka服务端会匹配请求类型,当匹配该请求是客户端发来寻找协调器的请求时(request.header.apiKey=ApiKeys.FIND_COORDINATOR)，会调用handleFindCoordinatorRequest方法:

```scala
request.header.apiKey match {
				....
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
  				....
}
```

handleFindCoordinatorRequest方法中会对判断请求授权相关信息后，创建__consumer_offsets。

```scala
def handleFindCoordinatorRequest(request: RequestChannel.Request) {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    if (findCoordinatorRequest.data.keyType == CoordinatorType.GROUP.id &&
        !authorize(request.session, Describe, Resource(Group, findCoordinatorRequest.data.key, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else if (findCoordinatorRequest.data.keyType == CoordinatorType.TRANSACTION.id &&
        !authorize(request.session, Describe, Resource(TransactionalId, findCoordinatorRequest.data.key, LITERAL)))
      sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else {
      // get metadata (and create the topic if necessary)
      val (partition, topicMetadata) = CoordinatorType.forId(findCoordinatorRequest.data.keyType) match {
        case CoordinatorType.GROUP =>
          val partition = groupCoordinator.partitionFor(findCoordinatorRequest.data.key)
          val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case CoordinatorType.TRANSACTION =>
          val partition = txnCoordinator.partitionFor(findCoordinatorRequest.data.key)
          val metadata = getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)

        case _ =>
          throw new InvalidRequestException("Unknown coordinator type in FindCoordinator request")
      }

     ....
  }
```

```scala
val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.context.listenerName)
```

```scala
private def getOrCreateInternalTopic(topic: String, listenerName: ListenerName): MetadataResponse.TopicMetadata = {
    val topicMetadata = metadataCache.getTopicMetadata(Set(topic), listenerName)
    topicMetadata.headOption.getOrElse(createInternalTopic(topic))
  }
```

```scala
  private def createInternalTopic(topic: String): MetadataResponse.TopicMetadata = {
   ....
    topic match {
      case GROUP_METADATA_TOPIC_NAME =>
        if (aliveBrokers.size < config.offsetsTopicReplicationFactor) {
          error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
            s"'${config.offsetsTopicReplicationFactor}' for the offsets topic (configured via " +
            s"'${KafkaConfig.OffsetsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
            s"and not all brokers are up yet.")
          new MetadataResponse.TopicMetadata(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, java.util.Collections.emptyList())
        } else {
          createTopic(topic, config.offsetsTopicPartitions, config.offsetsTopicReplicationFactor.toInt,
            groupCoordinator.offsetsTopicConfigs)
        }
      case TRANSACTION_STATE_TOPIC_NAME =>
        if (aliveBrokers.size < config.transactionTopicReplicationFactor) {
          error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
            s"'${config.transactionTopicReplicationFactor}' for the transactions state topic (configured via " +
            s"'${KafkaConfig.TransactionsTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
            s"and not all brokers are up yet.")
          new MetadataResponse.TopicMetadata(Errors.COORDINATOR_NOT_AVAILABLE, topic, true, java.util.Collections.emptyList())
        } else {
          createTopic(topic, config.transactionTopicPartitions, config.transactionTopicReplicationFactor.toInt,
            txnCoordinator.transactionTopicConfigs)
        }
      case _ => throw new IllegalArgumentException(s"Unexpected internal topic name: $topic")
    }
  }
```

也就是说，在客户端第一次向服务端发起寻找Coodinator请求时,并且请求中CoordinatorType为GROUP时，在没有__consumer_offsets，服务端会进行创建。那么，客户端在什么时候会发起FindCoodinatorRequest呢？

在KafkaConsumer的poll()方法中，会发送FindCoodinatorRequest请求。来向服务端寻找协调器。

## KafkaConsumer.java

> poll()

`client.maybeTriggerWakeup()`拉取消息之前 先校验用户是否进行了唤醒跳出poll循环操作。

`updateAssignmentMetadataIfNeeded(timer)`更新分配给消费者的元数据,心跳,位移等

```java
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
	...
            } while (timer.notExpired());
```

> updateAssignmentMetadataIfNeeded()

`coordinator.poll(timer)`发送请求，与服务端交互，获取数据，确保协调器相关数据，并且确定此消费者已经加入消费者组。

```java
boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        if (coordinator != null && !coordinator.poll(timer)) {
            return false;
        }
        return updateFetchPositions(timer);
    }
```

## ConsumerCoordinator.java

> poll()

`ensureCoordinatorReady(timer)`确保协调器已经准备好接收请求

```java
    public boolean poll(Timer timer) {
        maybeUpdateSubscriptionMetadata();

        invokeCompletedOffsetCommitCallbacks();

        if (subscriptions.partitionsAutoAssigned()) {
            pollHeartbeat(timer.currentTimeMs());
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            if (rejoinNeededOrPending()) {
                if (subscriptions.hasPatternSubscription()) {
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        this.metadata.requestUpdate();
                    }

                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    maybeUpdateSubscriptionMetadata();
                }

                if (!ensureActiveGroup(timer)) {
                    return false;
                }
            }
        } else {
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }
        }

        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }
```

> ensureCoordinatorReady()

`lookupCoordinator();`查找组协调器GroupCoordinator

```java
protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        if (!coordinatorUnknown())
            return true;

        do {
            final RequestFuture<Void> future = lookupCoordinator();
            client.poll(future, timer);

            if (!future.isDone()) {
                break;
            }

            if (future.failed()) {
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata");
                    client.awaitMetadataUpdate(timer);
                } else
                    throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                markCoordinatorUnknown();
                timer.sleep(retryBackoffMs);
            }
        } while (coordinatorUnknown() && timer.notExpired());

        return !coordinatorUnknown();
}
```

> lookupCoordinator()

`this.client.leastLoadedNode();`找最近的节点 查协调器位置。

`sendFindCoordinatorRequest(node)`发送寻找组协调器的请求

```java
    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
        }
        return findCoordinatorFuture;
    }
```

如前文所说,当服务端收到DindCoordinatorRequest请求，会创建__consumer_offsets。

## 提交位移请求响应结构

### 客户端提交位移发送OffsetCommitRequest请求

```json
请求结构为
Header: 
	ApiKey => int16 API调用类型的Id kafka会匹配不同的APIKEY来进行不同的操作
	ApiVersion => int16 API的版本号，该版本号允许服务器根据版本号正确地解释请求内容。响应消息也始终对应于所述请求的版本的格式
	CorrelationId => int32 用户提供的整数，匹配客户机和服务器之间的请求和响应
	ClientId => string 客户端自定义标识
Body(kafka-V2版本)
	ConsumerGroupId => string 消费者组ID
	ConsumerGroupGenerationId => int32 消费者组的年代信息,类似消费者组的版本号，避免受到过期请求影响。
	ConsumerId => string 消费者ID
	RetentionTime => int64 当前消费位移所保留的时长，对于消费者而言，这个值保持为-1，也就是取Broker端的offsets.retention.minutes 参数值来确定保留时长，默认10080，即七天，超出删除。
	Topics => array 
		[
			name => String topic名称
			Partitions => array
				[
					partitionIndex => int partition号
					committedOffset => Long 64位长整型数 位移
					committedLeaderEpoch => int 提交版本号 纪元
					commitTimestamp => Long 时间戳
					committedMetadata => String 自定义元数据
				]
		
		]
```

### Offset Commit Response响应

```json
提交位移响应结构
Header: 
	ApiKey => int16 API调用类型的Id kafka会匹配不同的APIKEY来进行不同的操作
	ApiVersion => int16 API的版本号，该版本号允许服务器根据版本号正确地解释请求内容。响应消息也始终对应于所述请求的版本的格式
	CorrelationId => int32 用户提供的整数，匹配客户机和服务器之间的请求和响应
	ClientId => string 客户端自定义标识
Body(kafka-V2版本)
	TopicName => string topic名称
	Partition => int32 partition号
	ErrorCode => int16 错误码
```

## 查看指定消费者组对应__consumer_offsets分区的内容:

计算group在__consumer_offsets所在分区

`Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount`

```shell
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic __consumer_offsets --partition x --formatter 'kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter'

```









































