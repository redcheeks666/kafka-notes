package com.example.demo.topic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CuonsumerOffset {
    public static final String brokerList="ha01:9092,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";
    public static final String groupId="group.demo";
    //设置kafkaConsumer对应客户端id 默认"",如果不设置,kafkaconsumer自动分配形式如"consumer-1"字符串
    public static final String clientId="client.id";

    public static Properties initProp(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,clientId);
        return properties;
    }

    public static void main(String[] args) {
        Properties prop = initProp();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        TopicPartition topicPartition = new TopicPartition(topic,0);
        //订阅
        consumer.assign(Arrays.asList(topicPartition));

        //当前消费位移
        long currentOffset=-1;

        //循环消费
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            //消息为空跳出循环消费
            if (records.isEmpty()){
                break;
            }
            /**
             * Get just the records for the given partition
             * 只获取指定分区的数据
             * @param partition The partition to get records for
             */
            List<ConsumerRecord<String, String>> partitionRecord = records.records(topicPartition);
            /**
             * partitionRecord.size()-1 分区消息总数-1 最后一条消息的位移 即当前消费位移
             * */
            currentOffset= partitionRecord.get(partitionRecord.size() - 1).offset();
            for (ConsumerRecord<String, String> record : partitionRecord) {
                System.out.println(record.value());
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception==null) {
                        Set<TopicPartition> topicPartitions = offsets.keySet();
                        topicPartitions.forEach(x-> System.out.println(offsets.get(x)));
                    }
                }
            });
        }

        System.out.println("当前消费位移");
        System.out.println(currentOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
        System.out.println("提交的位移");
        System.out.println(offsetAndMetadata.offset());
        long position = consumer.position(topicPartition);
        System.out.println("下次消费位移");
        System.out.println(position);
    }
}
