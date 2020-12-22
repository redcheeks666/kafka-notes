# kafka日志分析

* [kafka日志分析](#kafka日志分析)
    * [kafka的LOG文件存储结构](#kafka的log文件存储结构)
    * [日志分段](#日志分段)
    * [日志实例格式演变](#日志实例格式演变)
      * [V0版本](#v0版本)
      * [v1版本](#v1版本)
      * [V2版本](#v2版本)
      * [Varints和ZigZag](#varints和zigzag)
      * [消息大小测试](#消息大小测试)
    * [消息压缩](#消息压缩)
      * [启用Kafka消息压缩的场景](#启用kafka消息压缩的场景)
      * [额外压缩消耗](#额外压缩消耗)
      * [消息压缩原理](#消息压缩原理)
    * [消息索引](#消息索引)
      * [偏移量索引](#偏移量索引)
      * [偏移量消息定位](#偏移量消息定位)
      * [时间戳索引](#时间戳索引)
      * [时间戳消息定位](#时间戳消息定位)
      * [二分法查找日志分段](#二分法查找日志分段)
      * [跳表结构](#跳表结构)
    * [消息清理](#消息清理)
      * [日志删除](#日志删除)
        * [基于时间的保留策略](#基于时间的保留策略)
        * [基于日志大小保留策略](#基于日志大小保留策略)
        * [基于偏移量的保留策略](#基于偏移量的保留策略)
      * [日志压缩](#日志压缩)
        * [使用场景](#使用场景)
        * [清理日志区域划分](#清理日志区域划分)
        * [挑选清理区](#挑选清理区)
        * [清理流程](#清理流程)
        * [清理位置源码](#清理位置源码)
        * [最小污浊率](#最小污浊率)
        * [合并小文件](#合并小文件)
    * [消息存储](#消息存储)
      * [缓存磁盘I/O](#缓存磁盘io)
      * [零拷贝](#零拷贝)
      * [IO调度算法](#io调度算法)
      * [kafka顺序写](#kafka顺序写)
      * [Kafka页缓存](#kafka页缓存)
      * [kafka零拷贝](#kafka零拷贝)

      ​

### kafka的LOG文件存储结构

![kafka的log目录架构](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\kafka的log目录架构.png)



比如，新建Topic "topic-a" 三个分区 三个副本因子 创建好之后，找到服务端配置的kafka-logs目录,文件结构如下。

> 该kafka-logs中除了这些还会有4个checkpoint检查点文件和1个meta.properties文件，当配置了多个kafka-logs目录时，Broker会挑选分区数较少的目录来创建当前的分区log文件夹。

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\kafka-logs目录结构.png)

每一个分区副本都对应的一个log文件夹。

每个文件夹中，包含多个LogSegment(日志分段)。

> topic-a-N 文件夹内

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\log内部文件结构.png)

​		如上图，一个.index文件，一个.timeindex文件，一个.log文件组成一个LogSegment。kafka是顺序写入的，所以每次新消息只写入最后一个logSegment,当最后一个LogSegment满足条件时，创建新的LogSegment来让新消息写入。

​		每一个LogSegment的第一条消息的偏移量为基准偏移量,此偏移量为64位长整型数,并且，每个LogSegment的三个文件(.index,.log,.timeindex)都以此偏移量命名。长度为20位,位数不足则补0。

### 日志分段

日志分段或者日志索引达到一定条件(满足以下其一)都会对日志进行切割，对应的索引文件也会进行切割。

1. 日志分段大小超过了Broker端参数log.segment.bytes的值，默认1073741824，即1GB。
2. 日志分段的最大时间戳大于log.roll.ms(优先级高，默认168，即七天)或log.roll.hours。
3. 偏移量索引文件或者时间戳索引文件大小达到Broker端参数log.index.size.max.bytes配置的值。log.index.size.max.bytes的默认值为10485760，即10M
4. 追加消息的偏移量与当前日志分段的偏移量差值大于Integer.MAX_VALUE，即要追加的偏移量不能转换为相对偏移量(offset-baseOffset>Integer.MAX_VALUE)

### 日志实例格式演变

#### V0版本

(kafka消息格式的第一个版本)

(Message Set)

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\kafka消息格式-V0版本.png)

> 图片摘自博客

1. src32		=> 4B crc32校验值,校验范围magic-value
  2. magic	=> 1B 消息格式版本号,此版本为0
  3. attributes=> 1B 消息属性,低三位(左高右低,即右数3位)表示压缩类型,0(000)表示NONE无压缩,1(001)表示ZIP,2(010)表示SNAPPY,3(011)表示LZ4,其余位保留(V2版本已弃用)
  4. keylength=> 4B 消息Key的长度，-1表示没有设置Key,即key=null
    5. key	=> 无key,则无此字段
  5. valuelength=> 4B 消息体长度,-1表示消息为空
    7. value=> 消息体 也可为空

排除key和value字段,其余字段加起来crc32 + magic + attributes + key length + value length = 4B + 1B + 1B + 4B + 4B =14B,也就是V0版本中消息最小14B，小于此值的消息则为破损消息而不被接收。

#### v1版本

(Message Set)

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\kafka消息格式-V1格式.png)

> 图片摘自博客

格式与V0类似,多了一个timestamp(8B)表示消息的时间戳。

magic为1,表示V1版本,attributes 低三位与V0版本一致,表示压缩类型,与V0不同的是左数第四个Bit用来表示时间戳类型,0表示timestamp时间戳类型为CreateTime(即消息创建时间),1表示timestamp时间戳类型为LOgAppendTime(即消息写入时间)
因为多了个timestamp(8B)字段,所以，V1版本的消息最小为22B

#### V2版本

(Record Batch)

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\kafka消息格式-V2格式.png)

> 图片摘自博客		

kafka0.11.0版本之后开始使用V2版本的消息格式，与之前版本相比，V2参考了Protocol Buffer引入了变长整型(Varints)和ZigZag编码，部分场景下，缩减了消息所占空间。格式上，V2版本把之前消息Record中的crc32字段移出到了外层，如果对于V1的版本，消息时间戳timestamp类型为LogAppendTime(进入系统时间)而不是CreateTime(消息创建时间)，那么，在进入Broker时，timestamp会进行更新的操作，引发重新校验crc32的值，而此值在Producer端已经被计算校验过了，或者，Broker端在进行消息格式转换时(如V1格式转V0格式)，crc32值也会变动，多次重复计算场景使得crc32变得冗余，遂转移到了外层。

​		V2在进行消息压缩时，除records外，是不被压缩的。压缩的只是records中的所有内容。

​	较之前版本区别字段分别为：

​	RecordBatch:

1. fist offset：表示RecordBatch的起始位移。
2. length：partition leader epoch 到headers之间的长度。
3. partition leader epoch : Leader时代的标识，存在每个RecordBatch中，确保消息可靠性。
4. magic：版本号。
5. attributes：消息属性，与之前不同的是，V2版本中此字段占2个字节，低三位还是表示压缩方式。第四位还是表示时间戳类型，第五位表示此RecordBatch是否处在事务中。第六位表示是否是Control消息,Control消息用来支持事务功能，1表示是Control消息，0表示非Control消息。
6. last offset delta：RecordBatch中最后一个Record的offset值与fist offset的差值，校验RecordBatch中Record组装的正确性。
7. first timestamp：RecordBatch中第一条Record的时间戳。
8. max timestamp：RecordBatch中最大的时间戳，一般是当前RecordBatch中最后一条Record的时间戳，也是确保Record组装正确性。
9. producer id：用来支持幂等。
10. producer epoch：用来支持幂等。
11. first sequence：用来支持幂等。
12. records count：RecordBatch中Record的个数。

   ​Records:

13. length：消息总长度。
14. attributes：弃用，作为预留字段。1B
15. timestamp delta : 时间戳增量(与外层RecordBatch中的时间戳差值)。通常时间戳为8个字节，保存差值的话，可以减少占用字节数。
16. offset delta ：位移增量(与外层起始位移的差值)。也可以节省所占字节数。
17. headers：Array，一个Record可以包含多个Header，Header包含Key-Value.作为应用级别扩展。

#### Varints和ZigZag

Varints

​		Varints是使用一个或多个字节序列化整型数的方法，数值越小，所占字节越小。Varints中，最高位bit表示在此字节是否和后一个字节表示同一个整数。1表示是，0表示此整数二进制的末位字节。剩下的七位表示数据本身。通常一个字节8位可以表示256个值，称为Base256。使用七位表示的话，可以表示128位。

> Varints使用小端字节序.
>
> 大端字节序：高位字节在前，低位字节在后，这是人类读写数值的方法。
>
> 小端字节序：低位字节在前，高位字节在后，即以0x1122形式储存。
>
> 为什么会有小端字节序？
>
> 计算机电路先处理低位字节，效率比较高，因为计算都是从低位开始的。所以，计算机的内部处理都是小端字节序。
>
> 人类习惯于读写大端字节序。所以，除了计算机的内部处理，其他的场合几乎都是大端字节序，比如网络传输和文件储存。

例如，用Vraints表示十进制300.

编码

十进制转二进制 `100101100`

按7位一组 从低位开始分组为

`000 0010` `010 1100`

小端字节序排序

`010 1100` `000 0010`

末位字节最高位补0，其余字节最高位补0。

`1010 1100` `0000 0010` (Varints)

解码

`1010 1100` `0000 0010` (Varints)

去除每个字节的最高位

`010 1100` `000 0010` (Varints)

小端字节序转大端字节序

`000 0010` `010 1100`(Varints)

2^8+2^5+2^3+2^2=300

ZigZag

​		Varints使用了ZigZag编码格式，ZigZag是一种锯齿形的方式穿梭正负值之间。可以使Varints对绝对值较小的负数有较小的字节占用。参考：

| 原始值         | ZigZag编码后的值 |
| ----------- | ----------- |
| 0           | 0           |
| -1          | 1           |
| 1           | 2           |
| -2          | 3           |
| 2           | 4           |
| ...         | ...         |
| 2147483647  | 4294967294  |
| -2147483648 | 4294967295  |

**对应的公式**：

​	(n << 1) ^ (n >> 31)=>sint32

​	(n << 1) ^ (n >> 63)=>sint64

例如,拿十进制 -1 进行ZigZag编码(sint32)

-1的二进制为

`1111 1111` `1111 1111` `1111 1111` `1111 1111` (补码，所占4个字节)

(n << 1) = `1111 1111` `1111 1111` `1111 1111` `1111 1110` (左移一位，低位补0)

(n >> 31) =`1111 1111` `1111 1111` `1111 1111` `1111 1111`(与左移不同，右移31位后，高位正数补0，负数补1)

(n << 1) ^ (n >> 31) = 1(取异或，相同0，不同1)

最终，原本需要四个字节表示，经过ZigZag编码后只需要1个字节。

​		如前文所说，Varints只有七位是有效数值位，可表示128个值，在经过ZigZag编码后(转变成绝对值之后)，一个字节只能表示64个(0~63)。

如，转换十进制64

1. 先将其转换成二进制： `1000000`
2. ZigZag编码处理：`1000 0000` ^ `0000 0000` = `1000 0000`
3. 每个字节低七位处理： `000 0001` `000 0000`
4. 小端字节序排序：`000 0000` `000 0001`
5. 补充最高位：`1000 0000` `0000 0001`

> Varints并非一直会省空间，一个int32最长会占用5个字节（大于默认的4字节），一个int64最长会占用10字节（大于默认的8字节）。

#### 消息大小测试

​		拿V2版本来实验，我们往kafka的topic=>topic-a(分区3,向1分区发送，副本3)发送一条key=“DemoKey”，value="DemoValue"的消息。

**查看日志**

```shell
[root@HA03 kafka_2.12-2.3.1]# bin/kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/topic-a/00000000000000000000.log --print-data-logDumping /tmp/kafka-logs/topic-a/00000000000000000000.log
```

打印结果

```shell
Starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 5 isTransactional: false isControl: false position: 0 CreateTime: 1599887411245 size: 84 magic: 2 compresscodec: NONE crc: 3888717251 isvalid: true
| offset: 0 CreateTime: 1599887411245 keysize: 7 valuesize: 9 sequence: -1 headerKeys: [] key: DemoKey payload: DemoValue
```

size字段显示总大小为84。

验证：

​		RecordBatch除去Record,其余总共61B大小。

​		Record中attributes占1B；timestamp delta值为0，占1B；offset delta值为0，占1B；key length值为7，占1B，key占7B；value length值为9，占1B，value占9B ; headers count值为0，占1B, 无headers。Record总长22B(编码为变长整型为1B),所以Record的length为1B。总大小23B。

​		61+23=84B。

### 消息压缩

#### 启用Kafka消息压缩的场景

1. **宽带不足，而CPU负载不高。最适合压缩，节约宽带。**
2. **CPU负载比较高，不适合消息压缩。**

```shell
#Broker
compression.type #默认为producer 即表示尊重producer的压缩方式
#Producer
compression.type #默认为NONE  表示不压缩
#gzip GZIP压缩算法
#snappy SNAPPY压缩算法
#lz4 LZ4压缩算法
#uncompressed 表示不压缩
```

#### 额外压缩消耗		

大部分情况，kafka会保持端到端的日志压缩，即Producer端压缩，Broker以压缩状态进行保存，Consumer拉取到的消息也是压缩状态，在处理消息时才会解压缩。也有部分情况例外，会进行额外的压缩/解压缩的资源消耗。如：

1. Broker和Producer指定了不同的压缩算法
2. Broker端发生了消息格式转换，为了兼容老版本，新版本的消息会转换成老版本的消息。

消息发生额外的压缩解压缩操作，不仅对CPU增加压力，还会使Kafka失去零拷贝的优势。

#### 消息压缩原理

由于kafka的V2消息格式与V1包括之前版本不同，所以压缩方式也发生了改变。

**V1版本消息压缩**

​		在V1版本中，kafka会将整个消息集压缩，作为外层消息的Value。外层消息除Value字段其余字段与内层消息格式一致。外层消息Key为null。

​		内层消息的offset从0开始,其实每个从Producer都是从0开始，类似于该消息集的相对位移，在Broker端进转换了绝对位移之后存入日志文件。外层保存了内层消息的最后一条消息的绝对位移，该绝对位移是相对于分区而言。

​		V1版本消息中的timestamp字段的对于压缩的区别：

1. 如果timestamp类型是CreateTime，那么外层消息设置的是内层消息的最大时间戳。内层消息还是消息的CreateTime。
2. 如果timestamp类似是LogAppendTime，那么外层消息设置的是系统当前的时间戳。内层消息时间戳都会被忽略，因为整个消息集的LogAppendTime是一致的。

**V2版本的消息压缩**

​		由于在V2版本的Record Batch消息格式中，record存的就是一或多条消息。所以在V2版本的压缩中，被压缩的是record中的所有消息。而其他字段是不会被压缩的。

### 消息索引

Kafka使用了两种索引,偏移量索引文件，时间戳索引文件，来定位消息位置，提高消息查询速率。偏移量索引来建立消息偏移量和消息物理位置之间的映射，时间戳索引建立了时间戳与偏移量的映射。

​	Kafka中索引文件以稀疏索引的方式构建消息索引，即并不保证每个消息在索引文件中都有对应的索引项，每当写入一定量的消息时(Broker端参数log.index.interval.bytes，默认4096，4KB)。偏移量索引文件和时间戳索引文件同时增加一个偏移量索引项和时间戳索引项。调整log.index.interval.bytes即可调整索引项的密度。稀疏索引是在磁盘空间，内存空间，查找时间等多方面的折中。

​	日志分段文件LogSegment进行切分时，日志索引文件也会进行切分。

​	稀疏索引通过MapperBytesBuffer将索引文件映射到内存中，加快索引的查询速度。

​	只要当前活跃的日志索引文件才可以写入，之前的文件会被设定为只读文件，索引文件的大小由Broker端参数log.index.size.max.bytes 设置。在创建新的活跃日志索引文件时，会为其预分配log.index.size.max.bytes 的空间。默认10485760，即10M。这也就是在新建topic的时候，会发现，日志LogSegment中，.log文件大小初始是0，而.index文件和.timestamp文件的初始大小为10485760。只有当索引文件进行切分时，kafka才会把索引文件裁剪到数据实际占用大小。

#### 偏移量索引

​	偏移量索引文件中的偏移量是单调递增的，查询指定的偏移量时，使用二分法定位偏移量位置，如果指定偏移量不在索引文件中，则会返回小于指定偏移量的最大偏移量。

​	偏移量索引项每个占8个字节，分两个部分。

1. relativeOffset:相对偏移量，表示消息相对于baseOffset(即当前索引文件名)的偏移量，占4个字节
2. position:物理位置，消息在日志分段中的物理位置，占4个字节。

  ​Kafka的消息绝对偏移量(offset)	占8个字节，但索引项中使用相对偏移量只占4个字节(relativeOffset=offset-baseOffset),这样设计减少了索引占的空间。

> 试想下，为何8个字节的绝对偏移量，使用4个字节的相对偏移量保存能否够用？
>
> 答案是肯定够的。
>
> 偏移量是使用int32类型保存的，4个字节最大值也就是Integer.MAX_VALUE(2147483647),前文所说，当日志分段大于1073741824B，即1GB时，日志文件就会被切分出新的日志分段。所以，4个字节的相对位移，是可以存储的下的。

我们往前文所创建的Topic中再次发送1000条消息。然后通过kafka-dump-log.sh脚本解析对00000000000000000000.index日志进行解析。

```shell
offset: 224 position: 5097
offset: 382 position: 9502
```

同样使用bin/kafka-dump-log.sh 脚本解析00000000000000000000.Log文件

```shell
baseOffset:	0	...	position:	0		...
baseOffset:	1	...	position:	84		...
baseOffset:	210	...	position:	5097	...
baseOffset:	225	...	position:	5503	...
baseOffset:	241	...	position:	5932	...
baseOffset:	244	...	position:	6062	...
baseOffset:	256	...	position:	6399	...
baseOffset:	365	...	position:	9012	...
baseOffset:	366	...	position:	9096	...
baseOffset:	381	...	position:	9502	...
baseOffset:	383	...	position:	9609	...
baseOffset:	384	...	position:	9693	...
```

#### 偏移量消息定位

kafka通过偏移量来定位消息位置的原理：

例如上面的日志中，我们要查找偏移量为360的消息。

1. 通过跳表结构查找对应的日志分段。即文件名不大于300的日志索引文件。
2. 二分法找到.index索引文件中索引项不大于300的索引项，即`offset: 224 position: 5097`。
3. 通过物理位置5097在索引文件对应的.log日志文件中找到position为5097的消息，即`baseOffset:210position:5097`。
4. 从baseOffset为210的位置开始顺序查找偏移量为360的消息。

#### 时间戳索引

时间戳索引文件中包含若干个时间戳索引项，每个索引项占12个字节，分两个部分。

1. timestamp:消息时间戳。8个字节。
2. relativeOffset:相对偏移量。4个字节。

  ​	每个追加的时间戳索引项中的timestamp必须大于之前追加索引项中的timestamp，否则不予追加。如果Broker端参数log.message.timestamp.type设置为LogAppendTime，则消息时间戳必定能保持单调递增;如果设置为CreateTime则无法保证时间戳单调递增。

  ​	当消息写入一定量时，偏移量索引项和时间戳索引项会同事增加一个，但两个索引项的relativeOffset并不一定是同一个值。

我们通过kafka-dump-log.sh脚本解析：

```shell
offset: 250 timestamp: 1601103010444
offset: 380 timestamp: 1601103072596
```

#### 时间戳消息定位

1. 通过时间戳在时间戳索引文件中，找到不大于该时间戳的最大时间戳索引项。
2. 通过时间戳索引项中的相对位移，在偏移量索引文件中找到不大于此偏移量的最大偏移量的物理位置，
3. 通过具体物理位置，从对应物理位置顺序查找对应消息。

#### 二分法查找日志分段

​		Kafka的消息新增，都是追加到文件的末尾，Consumer的消费，和Follower的同步数据，也是最常读取文件末尾。所以，kafka二分法查找并不是标准的二分法查找。而是通过’活跃区‘的概念对二分法进行改进查找。

​		对于kafka的二分法活跃区介绍，源码是这样解释的：

> kafka mmap将索引文件存入内存，该索引的所有读写操作都是通过OS页缓存进行的。大多数情况下，可以避免磁盘I/O阻塞。
>
> 据我们所知，所有现代操作系统都使用LRU策略或其变体方式，进行管理页面缓存
>
> LRU策略(
>
> 核心思想：如果数据近期访问过(read/write)，那么将来被访问的几率也会更高。
>
> 最常见实现方式：链表缓存
>
> 1. 新数据插入链表头部。
> 2. 被访问到的数据重新插入到链表头部。
> 3. 链表满时，丢弃尾部数据。)
>
> kafka所有日志的新增都会发生在尾部，消费者和Follower的数据同步也会在尾部操作。因此，LRU策略按道理来讲应该是可以和Kafka索引模式很好的结合。
>
> 但是，当查找索引时，标准的二进制对缓存并不友好，可能导致不必要的页面错误(因为一些不是尾部数据可能不在内存中，所以可能发生线程堵塞，等待从磁盘读取某些索引条目。)
>
> 例如：在具有是13页的索引中，现在已经写到了12页，查找时，标准的二进制算法会读取第0、6、9、11和12页中的索引项。
>
> 页号：| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
> 步骤：| 1 |	|     |    |    |	 | 3 |	 |    | 4 |    | 5  | 2/6 |
>
> 在每个页面中，有数百个日志项，对应成百上千kafka消息。当索引从12页第一个索引项写到12页最后一个索引项时，所有写操作都在12页中，所有的消费者查找，Follower同步数据时，都检索了第0,6,9,11,12页。由在12页开始写到结束前，每次查找都会检索这些页面，我们可以假设这些页面存在页面缓存中。
>
> 当索引项增长到了13页，同步数据和Consumer消费数据所需要的检索的页面就变成了 0 , 7 ,10,12和13页。
>
> 页号：| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 |
> 步骤：| 1 |    |    |    |     |     |    |  3 |  |     | 4    | 5   | 6 | 2/7 |
>
> 其中第7和第10页已经很久没有访问了，与其他页面比，他们在页缓存中的几率较小。增加第13页的第一个索引项时，第一次查找可能从磁盘读取了第7页第10页，这可能需要1秒的时间，在我们的测试中这可能导致的延迟从几毫秒升到大约1秒。
>
> 这里，我们使用了一种便于缓存的查找方法。
>
> if（target> indexEntry [end-N]）//如果目标位于索引的最后N个条目中，
> binarySearch（end-N，end）
> else
> binarySearch（begin，end-N）
>
> 如果可能，我们只需要查找索引的最后N个项。所有的同步查找都应该属于`binarySearch（end-N，end）`，我们将最后N个项称为’温暖‘区域。当我们经常在这个较小的区域进行检索，包含此部分的页面更可能存于缓存。
>
> 我们将N（_warmEntries）设置为8192，因为；
>
> 1. 这个数字足够小，可以确保在每个温暖区查询时，都能检索到温暖区的所有页面，这样，整个温暖区都是温暖的，即温暖区的页面都会在页缓存中。在进行温暖区查询时，始终会检索到三个项，indexEntry（end）末尾，indexEntry（end-N）起始和indexEntry（（end * 2-N）/ 2）中间项。如果页面大小>=4096,则当我们检索这三个项时，所有温暖区的页面（三个或更少）都会被检索。截止到2018年，4096是所有处理器（x86-32，x86-64，MIPS，SPARC，Power，ARM等）的最小页面大小。
>
> 2. 这个数字足够大，以确保大多数的同步数据检索都在热区，使用Kafka的默认设置，8KB的索引项。
>
>    8KB偏移量索引项：8192/8B(每个偏移量索引项8个字节)*4KB(broker端参数log.index.interval.bytes,默认4KB，即每增加4KB的消息，新增一个偏移量索引项)，即4Mkafka消息。
>
>    8KB时间戳索引项：8192/12B(每个时间戳索引项12个字节)*4KB(broker端参数log.index.interval.bytes,默认4KB，即每增加4KB的消息，新增一个偏移量索引项)，即2.7MBkafka消息。
>
> 我们无法将N(_warmEntries)设置大于8192，因为没有简单的方法保证典型的4KB页面主机上温暖区所有的索引项都是温暖的。
>
> 在将来，我们可以使用后端线程来定期触摸热区所有的索引项，这样，我们就可以。
>
> 1)支持较大的暖区。
>
> 2)确保低QPS主题分区内的索引项都是温暖的。

kafka中改进二分法查找日志源码

```java
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
        // 第1步：如果索引为空，直接返回<-1,-1>对
        if(_entries == 0)
          return (-1, -1)
    
        // 封装原版的二分查找算法
        def binarySearch(begin: Int, end: Int) : (Int, Int) = {
          // binary search for the entry
          var lo = begin
          var hi = end
          while(lo < hi) {
            val mid = (lo + hi + 1) >>> 1
            val found = parseEntry(idx, mid)
            val compareResult = compareIndexEntry(found, target, searchEntity)
            if(compareResult > 0)
              hi = mid - 1
            else if(compareResult < 0)
              lo = mid
            else
              return (mid, mid)
          }
          (lo, if (lo == _entries - 1) -1 else lo + 1)
        }
    
        // 第3步：确认热区首个索引项位于哪个槽。_warmEntries就是所谓的分割线，目前固定为8192字节处
        // 如果是OffsetIndex，_warmEntries = 8192 / 8 = 1024，即第1024个槽
        // 如果是TimeIndex，_warmEntries = 8192 / 12 = 682，即第682个槽
        val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries)
        // 第4步：判断target位移值在热区还是冷区
        if(compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
          return binarySearch(firstHotEntry, _entries - 1) // 如果在热区，搜索热区
        }
    
        // 第5步：确保target位移值不能小于当前最小位移值
        if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
          return (-1, 0)
 
        // 第6步：如果在冷区，搜索冷区
        binarySearch(0, firstHotEntry)
```



#### 跳表结构

kafka使用了跳跃表的结构来查找日志分段，即使用空间换时间的高效，KAfka的每个日志对象使用了ConcurrentSkipListMap来保存日志分段，每个分段的baseOffset作为Key。这样可以根据指定偏移量快速定位消息。

### 消息清理

kafka提供了两种日志清理的策略。

1. 日志删除(Log Retention)：按一定的保留策略直接删除不符合条件的日志分段。
2. 日志压缩(Log Compaction)：针对相同key的不同消息，只保留后一个版本。

delete

```shell
#设置日志清理策略
log.cleanup.policy#默认值为delete
```

compaction

```shell
#compaction
log.cleaner.enable true
log.cleanup.policy compact
```

log.cleanup.policy还可以设置为“delete，compact”，同时支持日志删除和日志压缩两种策略。

更细粒度的还有主题级别参数cleanup.policy。

无论是哪一种删除策略，其删除流程是一致的，即：

1. 先从Log对象维护的日志分段的跳跃表中把这些待删除的分段去除，保证线程对这些分段没有操作。
2. 所有待删除的日志分段对应的所有文件都加上`.deleted`的后缀
3. 最后以一个名为‘delete-file’命名的延迟任务删除这些以.deleted为后缀的文件。延迟任务时间设置为file.delete.delay.ms,默认60000，即1分钟。

#### 日志删除

在Kafka的日志管理器中会专门启动一个日志检测定时任务，周期性检测不符合保留条件的日志分段。

```shell
#Broker
log.retention.check.interval.ms#默认300000 5分钟
```

当前日志分段的保留策略有三种，基于时间的保留策略，基于日志大小的保留策略，基于偏移量的保留策略。

##### 基于时间的保留策略

定时任务通过判断设定的阈值，relentionMs来寻找可删除的日志分段集合deletableSegment。

```shell
#relentionMs
log.retention.ms#优先级最高
log.retention.minutes#默认168 即七天
log.retention.hours
```

跟阈值对比的不是日志分段的最近修改时间lastMotifiedTime,而是和日志分段中最大时间戳largestTimeStamp，因为最后修改时间可以被修改，如执行了touch操作，分区重分配。要查找largestTimeStamp首先，先查找对应的时间戳索引文件，找到时间戳索引文件中最后一条索引，若值大于0，则取，否则去日志最后修改时间。

##### 基于日志大小保留策略

通过查看日志文件大小是否超过阈值relentionSize，寻找可删除的deletableSegment日志分段集合。

```shell
#relentionSize
#log中所有日志文件的大小，而不是单个日志分段
log.retention.bytes#默认值为-1 表示无穷大。
```

查找流程

1. 先拿日志文件总大小size和relentionSize差值。
2. 然后从第一个日志分段开始把可删除的加入deletableSegment中，直到剩余的文件小于阈值。

##### 基于偏移量的保留策略

​		一般情况下，日志的起始偏移量logStartOffset等于第一个日志分段的baseOffset，部分情况，如logStartOffset可通过DeleteRecordRequest请求,日期清理,截断等操作进行修改。

​		把不大于logStartOffset的baseOffset偏移量的日志分段加入deletableSegment中，等待删除。

#### 日志压缩

日志压缩是指Kafka对日志删除Log Retention规则之外提供的一种日志清理策略Log Compaction。

Log Compaction对于相同的Key值不同的Value，kafka只保留最后一个版本。

**kafka中用于保存消费者位移的__consumer_offset使用的就是Log Compaction策略**

##### 使用场景

​		在一些实时计算中，容灾和存储方面有很好的应用，比如在spark,flink中，常常需要在内存中维护一些数据，这些数据可能聚合了一定周期的数据，而每次故障恢复需要重新计算，效率低，往往的选择是把中间计算结果使用Redis或者Mysql存储在磁盘中，而使用kafka来代替这些存储介质。就可以有很好的优势，比如kafka即可以当做数据源，又可以作为存储介质，简化技术栈，降低维护成本。使用外部存储，需要把key记录下来，通过key将数据取回，实现复杂度高。最关键，kafka使用磁盘顺序读写，可以高性能实现。

##### 清理日志区域划分

​		可以将每个日志分为两个部分，之前已经清理过的Clean干净部分，和尚未清理的dirty污浊部分，其中污浊部分又可以分为两个部分，可清理部分和不可清理部分。不可清理部分包括，活跃日志分段和如果设置了压缩滞后时间，日志分段的最大时间戳在压缩滞后时间内的消息。

```shell
#日志滞后时间，即日志最小保留时间
log.cleaner.min.compaction.lag.ms#默认值为0
```

##### 挑选清理区		

日志清理任务是通过后台线程池实现的，每个线程会挑选最污浊的日志进行清理，污浊区的日志大小/污浊区加清理过的区的总大小。污浊率最高的分区会被进行清理。

```shell
#清理线程数
log.cleaner.thread #默认为1
```

```scala
//污浊率计算
val totalBytes = cleanBytes + cleanableBytes
val cleanableRatio = cleanableBytes / totalBytes.toDouble
```

##### 清理流程

​		kafka在清理过程中，会遍历两次日志文件，第一次把每个Key的哈希值和最后出现的offset保存在`SkimpyOffsetMap`中，第二遍会把每个的offset和在`SkimpyOffsetMap`对应Key的offset对比，如果不小于`SkimpyOffsetMap`中对应的偏移量，则保留。

##### 清理位置源码

```scala
/**
  * Helper class for a log, its topic/partition, the first cleanable position, the first uncleanable dirty position,
  * and whether it needs compaction immediately.
  */
private case class LogToClean(topicPartition: TopicPartition,
                              log: Log,
                              firstDirtyOffset: Long,
                              uncleanableOffset: Long,
                              needCompactionNow: Boolean = false) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum
  val (firstUncleanableOffset, cleanableBytes) = LogCleaner.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset)
  val totalBytes = cleanBytes + cleanableBytes
  val cleanableRatio = cleanableBytes / totalBytes.toDouble
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}
```

​		每次清理的起始位置为`firstDirtyOffset`，结束位置为`uncleanableOffset`。每次清理完，把清理到的位置更新在cleaner-offset-checkpoint文件中，已被下次清理作为`firstDirtyOffset`。

​		默认情况下，SkimpyOffsetMap使用MD5计算Key的Hash值，占空间大小16B，根据hash值来找SkimpyOffsetMap中的对应槽位，如果发生冲突，则使用线性探测法处理，为防止Hash冲突频繁，可调整Broker端参数log.cleaner.io.buffer.load.factor(默认值为0.9)来调整负载因子。

```shell
#负载因子
log.cleaner.io.buffer.load.factor#默认0.9
```

​		偏移量占大小8B，hash16B，所以一个映射项占24B，每个日志清理线程的SkimpyOffsetMap所占为log.cleaner.dedupe.buffer.size/log.cleaner.thread,默认情况下为128/1，也就是128M。所以默认情况下，可以保存128*0.9/24B，也就是大概503万个Key。

```shell
log.cleaner.dedupe.buffer.size #默认128M
```

##### 最小污浊率

​		为了防止日志不必要的频繁清理操作，kafka还使用了参数可以限制最小清理污浊率。默认为0.5。

```shell
#最小污浊率
log.cleaner.min.cleanable.ratio #默认0.5
```

##### 合并小文件

​		Log Compaction执行后的日志分段大小会比原先的分段小，为了避免过多的小文件出现，kafka实现了这样一个规则，如果连续段的日志和索引大小小于清理开始前的最大日志和索引大小，则在进行清理时合并连续的段。

​		LogCompaction执行过程中，会把需要保留的消息复制到一个.clean为后缀的临时文件中，此文件以第一个日志分段的文件名命名。过后，将.clean文件修改为.swap后缀的文件。然后删除原本的日志文件。过程中索引的转换也是如此。

```shell
#清理前最大日志分段大小
log.segment.bytes#默认值为1GB
#清理前的最大日志索引大小
log.index.interval.bytes#默认值为10MB。
```

### 消息存储

​		在存储设备的话题中，磁盘一直都是一个尴尬的词汇，而kafka又是以磁盘这种持久化形式来存储的。那kafka是如何提供如此高的性能呢？

#### 缓存磁盘I/O

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\磁盘IO分层图.png)

> 图片摘自知乎

VFS**：虚拟文件系统层，一般的文件系统是工作在内核态的，如ext2,ext3,ext4等。也有少部分的用户态文件系统，如，fuse。Linux为了方便管理这些不同的文件系统，提出了一个虚拟文件系统VFS的概念，对这些不同的文件系统提供一套统一的堆外接口。

**PageCache**：在HDD时代，内核和磁盘速度差异巨大，Linux内核引入了页高速缓存(PageCache)，把磁盘抽象成一个个固定大小的连续Page，通常为4K。对于VFS来说，只需要与PageCache交互，无需关注磁盘的空间分配以及是如何读写的。

**Mapping Layer**：映射层，即具体的文件系统层。

**Generic Block Layer**：通用块层,意义和VFS层一样，屏蔽底层不同设备驱动的差异性,提供统一的,抽象的通用块层API。通用块最核心的数据结构是Bio。通用块会根据I/O请求构造一个或多个Bio并提交给调度层。

**I/O Scheduler Layer**：I/O调度层,介于通用块和块设备驱动层之间,调度层主要为了减少磁盘I/O,增大磁盘整体吞吐量。会把通用块层提交的Bio进行排序或合并，提供多种I/O调度算法，支持不同场景应用。

**Block Device Layer**：块设备驱动层,每一类设备都有其驱动程序,负责设备的读写,I/O调度层的请求也会交给相应的设备驱动程序进行读写。大部分的磁盘驱动程序都采用DMA方式进行数据传输。(Direct Memory Access，直接存储器访问)。

DMA控制器自行在内存和I/O设备间进行数据传输,传输完成再通过中断通知CPU。通常块设备的驱动程序已经集成在内核层,即使我们直接调用块设备驱动层代码,也还是要经过内核。

SPDK(Storage performance development kit) 由Intel发起,用于加速使用NVME，SSD为存储的应用软件加速库,其核心用户态,异步,无锁,轮寻方式的NVME驱动,较内核驱动,SPDK大幅度缩短了I/O流程,降低了I/O延迟,提升I/O性能。DISK Layer:物理设备层,如HDD,SSD,NVME磁盘设备。

**DISK Layer**：物理设备层,如HDD,SSD,NVME磁盘设备。

**Linux 磁盘 I/O机制**

**传统缓存I/O**

传统缓存I/O又称为标准I/O，大多数文件系统的默认操作都是缓存I/O。

在Linux的IO机制中，操作系统会把IO数据缓存在文件系统的页缓存(page cache)中。也就是说:

**读操作(缓存I/O)**:操作系统会先从内核的缓冲区查找，命中直接返回，否则。从磁盘中读取，然后数据会先拷贝到操作系统的内核的缓冲区中，最后才会从内核缓冲区拷贝到应用程序的地址空间。

**写操作(缓存I/O)**:把数据Copy到内核空间的缓存中，如果是异步，那么写操作就已经完成。操作系统会定期把放在页缓存中数据刷入磁盘。如果是同步，则会立即刷入磁盘。应用程序需等到数据被完全刷入磁盘才会收到返回。

以read为例：

**Linux传统的数据拷贝流程：**

如从某个磁盘读取文件：

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\缓存IO流程图.png)

1. DMA把磁盘文件拷贝到内核缓冲区。
2. CPU把数据拷贝到用户态缓冲区。
3. CPU又把数据从用户态缓冲区拷贝到内核协议栈开辟的sorket缓冲区。
4. DMA把数据从sorket缓冲区拷贝到网卡然后发出去。

**优点**：

1. 缓存I/O使用了操作系统内核缓冲区，一定程度上，分离了应用程序空间与内核空间和实际的物理设备。保护系统本身运行安全。
2. 减少读盘次数，提升性能。

**缺点**

1. 在缓存I/O机制中，DMA控制器可以把数据直接从磁盘读取到页缓存中，或者从页缓存直接写回到磁盘。但是不能直接从应用程序地址空间和磁盘间进行数据传输。因此，数据在传输过程中，需要在应用程序地址空间和页缓存之间进行多次数据Copy操作。这些数据拷贝操作带来的CPU以及内存开销是非常大的。

对于某些特殊的应用来说，避开操作系统内核缓冲区，

#### 零拷贝

零拷贝并不是字面意义上的0拷贝，而是在特殊环境下，去掉不必要的拷贝。再一个是优化整个拷贝的过程。

**为什么会有零拷贝？**

传统的 Linux 系统的标准 I/O 接口（read、write）是基于数据拷贝的，也就是数据都是 copy_to_user 或者 copy_from_user，这样做的好处是，通过中间缓存的机制，减少磁盘 I/O 的操作，但是坏处也很明显，大量数据的拷贝，用户态和内核态的频繁切换，会消耗大量的 CPU 资源，严重影响数据传输的性能，有数据表明，在Linux内核协议栈中，这个拷贝的耗时甚至占到了数据包整个处理流程的57.1%。

**零拷贝的种类：**

1. 用户态直接 I/O
2. mmap
3. sendfile
4. DMA 辅助的 sendfile
5. splice
6. 写时复制
7. 缓冲区共享
8. 高性能网络 I/O 框架——netmap

简单介绍一下三种零拷贝,直接I/O，mmap，sendfile。

**直接I/O**

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\直IO流程图.png)

应用程序或者运行在用户态下的库函数直接访问硬件设备。数据直接跨过内核进行传输，内核在整个数据传输过程中，除了必要的虚拟存储配置之外，不参加其他工作，这种方式能直接
越过内核，极大提升性能。
适用：在进程地址空间有自己的数据缓存机制的，也称为自缓存应用程序，如数据库管理系统就是典型的自缓存应用程序。
**缺点**：直接操作磁盘IO,应用程序需要自己处理数据才能落盘。由于CPU和磁盘的执行时间差距较大，造成资源浪费，可以和异步IO结合使用。

**mmap**

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\mmapIO流程图.png)

应用程序调用mmap，磁盘中的数据通过DMA控制器拷贝到内核缓冲区，操作系统会将内核缓冲区与应用程序共享，这样就不用往用户空间拷贝。由内核缓冲区拷贝到Socket缓冲区。
**缺点**：mmap在建立映射时必须指定映射区域，读写区域固定，不能以增长方式写。

**sendfile**

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\sendfile流程图.png)

sendfile系统调用在Linux 2.1被引入，目的简化通过网络在两个通道间的数据传输。sendfile是只发生在内核态的数据传输接口，没有用户态的参与，减少上下文切换。
在Linux2.4内核做了改进，DMA把数据拷贝到内核缓冲区后，向Socket缓冲区中追加当前数据在内核缓冲区的位置和偏移量。然后根据Socket中的位置和偏移量，
直接把数据从内核拷贝到网卡设备。这样，在内核中，便实现了零拷贝。
JAVA中的TransferTo()实现了Zero-Copy。
**缺点**：只适用不需要用户态操作的程序。

#### IO调度算法

大多数的块设备都是磁盘设备。可以根据设备的不同或应用程序的的特点来设置不同的IO调度算法。可通过调整IO调度算法来提升性能。
五种LinuxIO调度器
Linux2.4:
Elevator
Linux2.6:
DeadLine
Anticipatory(Linux.2.6.33被删除)
CFQ

**Elevator**

也称为电梯调度，核心即将上层的随机读写进行排序或简单的合并，提升IO效率 为每个设备维护一个查询的请求，每当有新请求，能合并就合并，不能合并就尝试排序，不能排序就
放到请求队列最后。

**DeadLine**
防止请求时间过长，而不能被处理。为读写I/O分别提供了FIFO队列，读队列最大等待时间为500ms，写队列最大等待时间为5s。读写队列优先级高。排序为
FIFO(read)>FIFO(write)>CFQ

**Anticipatory**

CFQ和DEADLINE优化的焦点在于零散IO，对于连续IO，如顺序读，并没有优化，为了满足随机IO和顺序IO混合的场景。Linux提供了Anticipatory调度算法，该算法在DEADLINE的基础上为每个读IO设置了6秒的等待时间窗口。如果6ms内操作系统收到了相邻位置的读IO请求，就可以立刻满足。
Anticipatory通过设置等待时间来获取更高性能，假如一个设备只有一个屋里查找磁头，(例如一个单独的SATA磁盘)，将多个随机的小写入流，合并为一个大写入流，(相当于随机写变顺序写)，使用这个原理延时读取/写入换取最大的读取/写入吞吐量。适用大多数环境。尤其读取/写入较多的环境。

**CFQ**

CFQ的全称Completely Fair Queuing 特点是按照IO请求的地址进行排序，不按先后顺序。
CFQ是默认的磁盘调度算法，对于通用的服务器,是最好的选择，因为他试图均匀分布对IO宽带访问。
CFQ为每个进程单独创建一个队列来管理该进程所产生的请求，也就是每个进程一个队列，各队列之间使用时间片进行调度，保证每个进程都能很好的分配到IO带宽。
另外，IO调度器每次执行一个进程的四次请求，在传统的SAS磁盘上，磁盘寻道花去了绝大多数的IO响应时间，CFQ的出发点是对IO地址进行排序，尽量少的磁盘旋转次数来满足尽可能对IO地址进行排序，尽量少的磁盘旋转次数来满足尽可能多的IO请求。
**优点**：在SAS磁盘大大提升吞吐量。
**缺点**：相比较NOOP的按先来后到，会出现先来的不一定会被优先满足，进而引发请求时间过长而不被处理。

**NOOP**

Noop算法全称No Operation ,实现了最简单的FIFO的队列，所有请求大致按照先来后到的顺序处理，NOOP也会在FIFO的基础上做了相邻IO请求合并。所以并不完全按照先来后到处理。

> 不同的IO调度算法，或者不同的IO优化方案，对KAFKA这类依赖磁盘运转的程序影响很大，可以根据不同的业务需求来来测试并选择合适的磁盘调度算法。
>
> 从系统层面分析，kafka操作的都是普通文件，并没有依赖特定的文件系统，但依然推荐使用EXT4或XFS，尤其是XFS，对于kafka的写入性能，XFS有更好的性能。

#### kafka顺序写

磁盘在效率问题上一直是一个尴尬词汇，而Kafka 依赖于文件系统（更底层地来说就是磁盘）来存储和缓存消息。

那kafka又是如何保证使用磁盘持久化的情况下来保证高的吞吐量呢？

磁盘，内存I/O效率对比：

![](https://zllcfy.oss-cn-beijing.aliyuncs.com/picture\磁盘写入速度对比.jpg)

> 图片摘自博客

如图，有关测试结果表明，一个由6块7200r/min的RAID-5阵列组成的磁盘簇的线性（顺序）写入速度可以达到600MB/s，而随机写入速度只有100KB/s，两者性能相差6000倍。操作系统可以针对线性读写做深层次的优化。

**预读(read-ahead)**:由于是顺序读写，每次读取的时候可以预先把下一批次的数据读入缓存。减少I/O

**后写(write-behind)**:将很多小的逻辑写操作合并起来组成一个大的物理写操作技术。

实验表明，顺序写盘的速度不仅比随机写盘的速度快，而且也比随机写内存的速度快。

#### Kafka页缓存

如前文所述，页缓存机制即把磁盘数据缓存到内存中，每次读取先查看数据是否存在Page Cache中，如果存在则直接返回数据，不存在才会读取磁盘。写入时，每次先把数据写入Page Cache对应的页中，操作系统会定期把脏页写入磁盘。

**kafka为什么要使用页缓存？**

1. 对于一个进程，其内部缓存的数据，很可能在Page Cache中也被缓存了一份，同一份数据缓存两份显然不合适，而页缓存又很难被禁止，除非使用直I/O方式(Direct I/O)。
2. 对于JVM来说，对象内存开销大，空间使用率低。
3. JVM的回收机制，随着堆内数据变多，会变得慢。
4. 程序宕掉，无需担心缓存数据丢失。

对于单纯运行Kafka的集群而言，首先要注意的就是**为Kafka设置合适（不那么大）的JVM堆大小**，为Kafka分配6~8GB的堆内存就已经足足够用了，将剩下的系统内存都作为page cache空间，可以最大化I/O效率。

Linux相关调节参数：

```shell
#lusher线程刷写磁盘参数
#flush的检查周期，默认500，即5秒
vm.dirty_writeback_centisecs
#PageCache中被标记脏页时间超过此值，则刷入磁盘，默认3000，即5分钟
vm.dirty_expire_centisecs
#PageCache脏页总大小占空闲内存的比例超过此值，则刷入磁盘，默认10。
vm.dirty_background_ratio
#PageCache脏页总大小占总内存的比例超过此值，则阻塞所有write()线程操作，强制每个进程将自己文件写入磁盘,默认20
vm.dirty_ratio
```

Linux会使用磁盘的一部分作为Swap分区，这样可以进行进程的调度，把当前非活跃进程调入Swa分区，把内存空出来交给活跃进程。对大量使用页缓存的Kafka来说，应该尽力避免这种内存的交换。

Linux调节Swap机制的参数：

```shell
#上限100，设置为1最适配Kafka
vm.swappiness
#上限100，表示积极使用Swap分区，把内存数据及时搬运到Swap中。
#下限0，表示任何情况下都不发生内存交换。但内存耗尽会终止某些进程。
```



#### kafka零拷贝

除了顺序写，页缓存等技术，Kafka还使用了零拷贝(Zero-Copy)技术进一步提升性能。依赖于操作系统底层的sendfile()函数实现。java层面依赖于FileChannal.transferTo()方法。

Kafka零拷贝即，通过DMA(Direct Memory Access)将文件内容复制到内核下的ReadBuffer中，把包含数据位置和长度的文件描述符加到Sorket Buffer中，DMA引擎直接将数据从内核传到网卡(协议引擎)，减少数据Copy，也减少了上下文切换。

