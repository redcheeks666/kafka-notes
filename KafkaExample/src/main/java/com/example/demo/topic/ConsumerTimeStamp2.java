package com.example.demo.topic;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerTimeStamp2 {
    private static final String brokerList="ha01:9092";
    private static final String topic="topic-demo";
    private static final String groupId="group.demo";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //获取分区信息
        Set<TopicPartition> assignment = new HashSet<>();
        do {
            kafkaConsumer.poll(1000);
            assignment=kafkaConsumer.assignment();
        }while (assignment.size()==0);

        //构建待查询时间戳map 时间戳是一天前
        HashMap<TopicPartition, Long> timeStamp = new HashMap<>();
        assignment.forEach(x->timeStamp.put(x,System.currentTimeMillis()-1*24*60*60*1000L));
        //通过时间戳拿到大于时间戳的第一条消息的位移
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(timeStamp);
        //拿到分区集
        Set<TopicPartition> topicPartitionSet = offsets.keySet();
        //seek指定消费
        topicPartitionSet.forEach(x->{if(offsets.get(x)!=null){kafkaConsumer.seek(x,offsets.get(x).offset());}});
        //开始消费
        ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.value());
        }


    }
}
