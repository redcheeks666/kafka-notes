package com.zll.kafka.reporter;


import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * kafka自定义监控获取每分钟流入消息数量
 * 此案例统计的是单个Broker的指标,如果计算集群，可通过累加实现。
 *
 * 配置信息,如各个Broker的ip,端口,AR,ISR一般无法通过JMX获取,但是可通过手动配置将ip加入监控,
 * 通过Kafka 连接的ZooKeeper来实现这些信息的获取，比如前面提及的/brokers/ids/＜brokerId＞节点获取ISR之类信息。
 * 对于一些硬件指标，如iowait、ioutil等可以通过第三方工具如Falcon、Zabbix来获取
 *
 */
public class JmxConnection {

    private MBeanServerConnection connection;

    private String jmxUrl;

    private String ipAndPort;


    public JmxConnection(String ipAndPort){
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
        JmxConnection jmxConn = new JmxConnection("localhost:9999");
        //初始化连接
        jmxConn.initConnect();

        System.out.println(jmxConn.getMsgInPerSec());

    }


}
