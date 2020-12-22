# kafka副本的HW，LEO的变更

* [kafka副本的HW，LEO的变更](#kafka副本的hwleo的变更)
  * [一\.LEO的变更](#一leo的变更)
    * [1\.Follower何时更新自己的LEO](#1follower何时更新自己的leo)
    * [2\.Leader何时更新本机的Follower\-LEO](#2leader何时更新本机的follower-leo)
  * [二\.HW的变更](#二hw的变更)
  * [三\.数据丢失](#三数据丢失)
  * [四\.数据不一致](#四数据不一致)
  * [五\.Leader Epoch](#五leader-epoch)
  * [六Kafka为什么不支持读写分离](#六kafka为什么不支持读写分离)



**LEO**：即日志末端位移(log end offset)，记录了该副本底层日志(log)中下一条消息的位移值。(最后一条的下一条，而非最后一条位移)。LEO是针对每一个副本而言的，即每个副本的LEO可能都不相同。

**HW**：水位线，HW之前的消息都是已确认备份的，对于同一个副本而言，其HW不会大于LEO。另外，虽然每个副本都有自己的HW，但Leader副本的HW是分区HW，消费者能拉取到的也是Leader分区HW之前的消息。

对Kafka的Follower副本而言，拥有两份LEO，一份是Leader节点所保存的Follower-LEO(Leader副本保存了所有Follwer副本的LEO)，另一份是Follower节点自己保存的LEO。

Leader根据自己节点的LEO更新HW，Follower根据Leader节点的LEO更新HW。

## 一.LEO的变更

##### 1.Follower何时更新自己的LEO

Follower每次从Leader拉取到消息后，数据写入自身Log，并更新自身LEO.

##### 2.Leader何时更新本机的Follower-LEO

Leader收到Follower副本发送来的拉取数据Fetch请求时，从自身Log拉取数据，并更新其Follower的LEO，然后返回给Follower响应。

## 二.HW的变更

1.Follower更新HW

Follower更新自身的HW发生在更新LEO之后，当Follower同步完日志，就会尝试更新自身的HW值。具体算法为（对比当前LEO的值和Fetch请求中Leader响应的HW值，取其最小）

2.Leader更新LEO

四种更新触发时机

- 分区Leader变更后。
- Broker故障导致ISR变更。
- Producer向Leader副本写入消息时。
- Leader处理Follower请求时。

这里，我们只探讨后两者，即在Broker正常工作下的Leader-HW的变更。Leader保存了所有Follower的LEO，当Leader尝试去更新HW时。会对比所有分区的LEO(处于ISR集的副本，这其中包括Leader自己的LEO)，选出最小的LEO作为HW（类似木桶短板理论）。

我们假设现有一个topic，单分区，双副本，min.insync.replicas=1,即一个Leader副本(简称L)，一个Follower副本(简称F)。

假设Producer发送消息之前，Leader和Follower的LEO和分区HW都为0。

**当Producer第一次发了一条消息后**

![Producer发送消息一](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Producer发送消息一.jpg)

1.Leader首先写入自己的Log。

2.Leader更新自己的LEO，并尝试更新HW(假设此时Follower还未发送Fetch请求)，此时Leader副本出F-LEO=0，自身的LEO=1,取其最小为0。与当前HW一致，遂不更新。

**当Follower向Leader发送了Fetch请求，试图同步数据**

![Follower向Leader同步数据二](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Follower向Leader同步数据二.jpg)

Leader收到Follower请求后

1.读取自身数据准备响应给Follower

2.根据Follower请求中的LEO更新F-LEO为0

3.尝试更新分区HW，此时HW为0，F-LEO为0，L-LEO为0。遂不更新，分区HW依旧为0.

4.把数据和当前分区的HW响应回Follower。



Follower收到Leader的响应后

1.同步Follower本地日志

2.更新Follower自身的LEO为1.

3.尝试更新分区HW值，此时本地HW为0，Leader返回的也为0.所以HW仍然为0

此时，Leader和Follower的第一轮交互已完成。虽然Follower已经同步了消息，且LEO为1，但分区的HW依旧为0，HW的更新在Follower与Leader的下一轮交互中完成。



此时，Follower发来第二轮Fetch请求。

![Follower向Leader同步数据二](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Follower向Leader同步数据三.jpg)

Leader收到请求后：

1.读取自身log准备响应给Follower(假设此时并没有新的消息写入,请求被寄存在Kafka的延时队列purgatory中，至超时被唤醒)

2.将请求中Follower所携带的LEO更新至F-LEO为1。

3.尝试更新分区HW，F-LEO为1，LEO为1，遂将分区HW更新为1。

4.把log和当前分区的HW响应回Follower



Follower收到请求后

1.同步日志至本地log。

2.尝试更新自身LEO，没有新数据，遂依旧为1.

3.尝试更新HW，对比自身LEO和HW，取最小，将HW更新为1。



Kafka的更新HW基本流程至此结束，但HW的更新存在空档期，即需要新一轮的Fetch请求才可以更新Follower副本的HW。此设计很明显会给数据造成丢失与不一致的情况。



## 三.数据丢失

数据丢失场景：

![Follower向Leader同步数据二](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture/Follower向Leader同步数据四.jpg)

还是之前的topic，单分区，双副本，min.insync.replicas=1,Leader副本A和Follower副本B，某一时刻，LeaderA副本中收到1条消息m1，FollowerB也同步了此消息，但未发起下一次Fetch请求，所以两个副本LEO都为1，但分区HW和FollowerB都为0 , 如果此时，FollowerB宕机了。那么FollowerB重启之时，根据HW截断日志，所以FollowerB中的m1消息被删除。之后再此向LeaderA发送Fetch请求，如果此时LeaderA宕机了，那么FollowerB就成为新的LeaderB，此时LeaderB的LEO为0，分区HW为0，当之前的LeaderA恢复后，成为FollowerA，根据HW进行日志截断，删除A中的m1，并将HW由1调整为0，如此m1消息不存在与AB两个副本。m1消息丢失了。

## 四.数据不一致

数据不一致场景：

![Follower向Leader同步数据二](C:\Users\asus\Desktop\副本HW变更\Follower向Leader同步数据五.jpg)

依旧时之前的topic，单分区，双副本，min.insync.replicas=1,Leader副本A和Follower副本B，某一时刻，LeaderA中收到了两条消息m1,m2，LEO为2，FollowerB中同步了其中一条m1，LEO为1，分区HW为2.FollowerB的HW为1，假如此时AB两副本同时宕机，FollowerB第一个恢复成为LeaderB，之后写入消息m3(此时LeaderB上存在消息m1,m3),并将分区HW更新为2。此时之前的LeaderA副本恢复成为FollowerA,在进行日志截断时，FollowerA本身的HW为2，A上面也只有两条m1,m2消息，所以无需调整。如此，A副本中消息为m1,m2，B副本中消息m1,m3,数据不一致。

## 五.Leader Epoch

为了解决以上两个问题，Kafka在1.11.0.0提出了Leader Epoch的概念，每次Leader的变更都会使对应的Leader Epoch自增，与此同时，每个副本还会增设一个矢量，`＜LeaderEpoch=＞StartOffset＞` ,StartOffset表示对应Leader Epoch时期写入的第一次条消息的位移。

在Kafka log的根目录，存放着四个检查点文件：

**cleaner-offset-checkpoint**

**log-start-offset-checkpoint**

**recovery-point-offset-checkpoint**

**replication-offset-checkpoint**

每个副本日志文件夹下也有一个检查点文件

**leader-epoch-checkpoint**

Kafka定时将LEO写入recovery-point-offset-checkpoint，定时将HW写入replication-offset-checkpoint，定时将每个分区的logStartOffset写入log-start-offset-checkpoint。

每次发送Leader Epoch，都会将`＜LeaderEpoch=＞StartOffset＞`写入leader-epoch-checkpoint

需要阶段日志时会根据Leader Epoch 的StartOffset作为参考，而不是HW。

**当应对数据丢失时**

例如，还是之前的场景，某个topic，单分区，双副本，min.insync.replicas=1，某一时刻，LeaderA副本中收到1条消息m1，FollowerB也同步了此消息，但未发起下一次Fetch请求，所以两个副本LEO都为1，但分区HW和FollowerB都为0 , 如果此时，FollowerB宕机了。那么FollowerB重启之时，不会再根据HW截断日志(如果根据HW截断，m1就会被删除)，而现在是携带当前的Leader Epoch值向LeaderA发送OffsetsForLeaderEpochRequest请求，LeaderA此时宕机了，那么FollowerB就会变为LeaderB,Leader Epoch就会从初始值0变为1，当LeaderA恢复为FollowerA时，向LeaderB发送OffsetsForLeaderEpochRequest请求并携带当前的Leader Epoch(0)，LeaderB收到请求后，根据Leader Epoch(0增1)返回Leader Epoch为1的的第一条位移(即m1消息的位移),FollowerA收到响应后便保存了m1消息。

**当应对数据不一致时**

例如，还是之前的场景，某个topic，单分区，双副本，min.insync.replicas=1，某一时刻，LeaderA中收到了两条消息m1,m2，LEO为2，FollowerB中同步了其中一条m1，LEO为1，分区HW为2.FollowerB的HW为1，假如此时AB两副本同时宕机，FollowerB第一个恢复成为Leader，之后写入消息m3(此时LeaderB上存在消息m1,m3),并将分区HW更新为2。此时LeaderA恢复成为FollowerA，携带当前的Leader Epoch值向LeaderB发送OffsetsForLeaderEpochRequest请求，LeaderB收到请求时返回Leader Epoch为1时的第一条消息，即m1，此时FollowerA收到响应后，删除m2，然后继续向LeaderB同步数据，最终两个副本消息都为m1,m3。

## 六Kafka为什么不支持读写分离

1,作为一个消息系统，大量的写操作数据一致性很难保证数据一致性。

2.作为一个消息系统，网络之间的传输很多，延时问题很难解决。





















