# Kafka定制化监控系统

* [Kafka定制化监控系统](#kafka定制化监控系统)
  * [一\.监控数据来源](#一监控数据来源)
  * [二\.监控消息堆积](#二监控消息堆积)
    * [获取Lag相关指标数据流程](#获取lag相关指标数据流程)
  * [三\.同步失效分区维度](#三同步失效分区维度)

## 一.监控数据来源

监控数据可通过JMX(Java Managent Extension,java管理扩展)来获取，使用JMX先开启kafka的JMX功能，默认关闭。

```shell
#启动时，通过配置JMX_PORT设置JMX的端口号，开启JMX功能
JMX_PORT=9999 nuhup bin/kafka-server-start.sh config/server.properties
```

kafka开启JMX之后，会在zookeeper的`/brokers/ids/<brokerId>`存在jmx_port对应的值。

对于kafka从JMX中获取指标数据，有两种方式

1.使用java自带的工具JConsole

2.使用java自带的JMX连接器(代码获取)

这里我们着重探讨第二种方式，在第二种方法中，要知道对应指标数据，首先要拿到指标对应的名称(MBean),这可以通过JConsole获取。

案例-代码获取MsgIn(Broker消息流入速度，全称MessagesInPerSec)指标值。

```java
/**
 * kafka自定义监控获取每分钟流入消息数量
 * 此案例统计的是单个Broker的指标,如果计算集群，可通过累加实现。
 *
 * 配置信息,如各个Broker的ip,端口,AR,ISR一般无法通过JMX获取,但是可通过手动配置将ip加入监控,
 * 通过Kafka 连接的ZooKeeper来实现这些信息的获取，比如前面提及的/brokers/ids/＜brokerId＞节点获取ISR之类信息。
 * 对于一些硬件指标，如iowait、ioutil等可以通过第三方工具如Falcon、Zabbix来获取
 *
 */
public class JmxConnectionDemo {

    private MBeanServerConnection connection;

    private String jmxUrl;

    private String ipAndPort;


    public JmxConnectionDemo(String ipAndPort){
        this.ipAndPort=ipAndPort;
    }


    //初始化jmx连接
    public Boolean initConnect(){
        //JMX连接格式
        jmxUrl="server:jmx:rmi:///jndi/rmi://"+ipAndPort+"jmxrmi";

        try {
            //构建连接
            JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);
            connection = connector.getMBeanServerConnection();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }


    //获取MsgIn指标的值
    public double getMsgInPerSec(){
        //拼接对应的MBean的名称
        String objectName="kafka.server:type=BrokerTopicMetrics, name=MessageInPerSec";

        Object oneMinuteRate = getAttribute(objectName, "OneMinuteRate");

        if (oneMinuteRate!=null){
            return (double)oneMinuteRate;
        }
        return 0.0;
    }


    /**
     * @param objName 指标属性名称
     * @param objArrt 统计维度,例如OneMinuteRate每分钟流入消息数
     * @return 根据MBean名称和属性获取具体值
     *
     */
    public Object getAttribute(String objName ,String objArrt){
        ObjectName objectName;


        try {
            objectName = new ObjectName(objName);

            return connection.getAttribute(objectName, objArrt);

        } catch (MalformedObjectNameException | MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }



    public static void main(String[] args) {

        //此处是kafka启动时,配置的jmx端口
        JmxConnectionDemo jmxConn = new JmxConnectionDemo("localhost:9999");
        //初始化连接
        jmxConn.initConnect();

        System.out.println(jmxConn.getMsgInPerSec());

    }


}
```



## 二.监控消息堆积

消息堆积是指存在于服务端消费者拉取未消费的消息。

对于每个分区而言，存在着LSO，HW，LEO三个消息位置概念：

**没有事务存在时**

LSO=HW≤LEO

**存在事务时**

LSO为事务中第一条消息的位置

存在事务时，LSO和HW可能会不同，由于kafka严格保证消息顺序，例如，一个Long Transaction事务A中的第一条位移为1000，另外存在几个其他已完成的事务，其偏移量为事务B（1100-1200），事务C（1500-1600）,假如此时LSO为1000，那么在事务A完成之前，已完成的事务BC对消费者同样不可见。（isolation.level参数为read_committed的情况）

所以，当存在事务时，LSO≤HW≤LEO。

我们在计算消息堆积时，当未存在事务时，我们的计算方法为Lag(消息堆积)=HW-ConsumerOffset,当存在事务时，我们计算消息堆积的算法为Lag=LSO-ConsumerOffset。

这里我们假设在所有消息都在非事务情况下，所以，我们的Lag=LSO-ConsumerOffset。



### 获取Lag相关指标数据流程

我们首先需要获取ConsumerOffset和HW的值。

在kafka-consumer-groups.sh脚本中，我们可以获取Lag的值。他的实现方式为

1.通过FindCoordinatorRequest请求查找对应的Coordinator。然后通过DescribeGroupsRequest请求获取消费者组元数据。

2.通过OffsetFetchRequest请求获取消费者位移Consumeroffset。

3.然后通过KafkaConsumer的endOffsets（`Collection＜TopicPartition＞partitions`）方法（对应于ListOffsetRequest请求）获取HW（LSO）的值。

4.最后通过HW与ConsumerOffset相减得到分区的Lag，要获得主题的总体Lag只需对旗下的各个分区累加即可



## 三.同步失效分区维度

处于同步失效或功能失效的副本(如非活跃状态副本)统称为失效副本，包含失效副本的分区统称为同步失效分区。





(未完)

[^]: 本文参考自《深入理解Kafka:核心设计与实践原理》



























