package com.zll.kafka.consumer;


import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>从某个时间节点开始消费</p>
 */
public class ConsumerSeekFromSomeTime {
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        //构建kafkaconsumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //订阅
        kafkaConsumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));

        //执行poll 拿到分配到的分区信息
        Set<TopicPartition> assignments = new HashSet<>();
        do {

            kafkaConsumer.poll(1000);
            assignments = kafkaConsumer.assignment();
        }while (assignments.size()==0);

        //构建一个map, key分区 value时间戳 timestamp取一天之前
        HashMap<TopicPartition, Long> partitionTimeStamp = new HashMap<>();
        for (TopicPartition topicPartition : assignments) {
            partitionTimeStamp.put(topicPartition,System.currentTimeMillis()-1*24*60*60*1000L);
        }

        //通过kafkaConsumer的offsetForTimes方法 传入map 拿到分区对应的offsetAndTimestamp  大于传入时间戳的第一条消息,对应的offset和timestamp
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(partitionTimeStamp);
        //拿到每个分区的位移数据offset  有了分区和位移 seek()就可以指定消费
        for (TopicPartition topicPartition : assignments) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            if (offsetAndTimestamp!=null)kafkaConsumer.seek(topicPartition,offsetAndTimestamp.offset());
        }
        //拉取数据
        ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.value());
        }
    }
}
