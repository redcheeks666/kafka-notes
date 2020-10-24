package com.example.demo.topic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerComitSync {
    //写一个以上节点即可,kafka会自动寻找集群其他节点地址
//    public static final String brokerList="47.95.210.165:2181";
    public static final String brokerList="47.95.210.165:9092";//,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";
    public static final String groupId="group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    //初始化配置
    public static Properties initProPerties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }
    //消费逻辑
    public static void main(String[] args) {
        //初始化配置
        Properties properties = initProPerties();
        //构建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        //分区粒度消费同步提交位移
        while (isRunning.get()){
            /**
             * 消息实例集(包含topic partition信息) map 集 < TopicPartition, List<ConsumerRecord> >
             * @TopicPartition topic partition
             * @List<ConsumerRecord> 消息集 包含key value offset timestamp ..等
             * */
            ConsumerRecords<String, String> records = consumer.poll(1000);
            //record.partitions keyset()获取所有key()，即此次拉取到的所有 TopicPartition<topic,partition>集
            Set<TopicPartition> partitions = records.partitions();

            for (TopicPartition topicPartition : partitions) {
                List<ConsumerRecord<String, String>> consumerRecordList = records.records(topicPartition);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecordList) {
                    System.out.println(consumerRecord.value());
                }
                long maxOffset = consumerRecordList.get(consumerRecordList.size() - 1).offset();
                consumer.commitSync(Collections.singletonMap(topicPartition,new OffsetAndMetadata(maxOffset+1)));
            }
//            consumer.close();
        }

    }
}
