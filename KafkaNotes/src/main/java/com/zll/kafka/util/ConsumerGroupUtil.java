package com.zll.kafka.util;


import com.zll.kafka.entities.PartitionAssignmentState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ConsumerGroupUtil {

    //创建Kafka ConsumerGroupUtil实例,通过KafkaConsumer.endOffsets()方法获取HW
    static KafkaConsumer<String,String> createNewConsumer(String brokerUrl, String groupId){

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        return new KafkaConsumer<String, String>(props);
    }


    //打印最终的输出结果的方法
    //%-ns表示左右对齐长度。
    public static void printPasList(List<PartitionAssignmentState> partitionStates){
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s %-30s %s ",
                "TOPIC",
                "PARTITION",
                "CURRENT-OFFSET",
                "LOG-END-OFFSET",
                "LAG",
                "CONSUMER-ID",
                "HOST",
                "CLIENT-ID"
        ));


        partitionStates.forEach(pState->{
                    System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s %-30s %s ",
                            pState.getTopic(),
                            pState.getPartition(),
                            pState.getOffset(),
                            pState.getLogSize(),
                            pState.getLag(),
                            Optional.ofNullable(pState.getClientId()).orElse("-"),
                            Optional.ofNullable(pState.getHost()).orElse("-"),
                            Optional.ofNullable(pState.getClientId()).orElse("-")
                    ));
                }
        );

    }

}
