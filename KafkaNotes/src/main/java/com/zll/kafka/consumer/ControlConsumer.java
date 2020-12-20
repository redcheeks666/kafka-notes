package com.zll.kafka.consumer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>控制消费</p>
 * 暂停消费的分区集
 * Set<TopicPartition> paused();
 * 暂停消费分区
 * void pause(Collection<TopicPartition> partitions);
 * 恢复分区消费
 * void resume(Collection<TopicPartition> partitions);
 *
 *  auto.offset.reset=latest (默认)从分区末尾开始消费
 *
 *  auto.offset.reset=earliest 从分区起始处消费
 *
 *  auto.offset.reset=none 查不到位移抛异常NoOffsetForPartitionException
 *
 *  void seek() 指定位移消费
 *
 * */
public class ControlConsumer {
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);


    public static Properties initProperties(){
        Properties properties = new Properties();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }


    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));

        TopicPartition topicPartition = new TopicPartition(KafkaProperties.TOPIC, 0);

        while (isRunning.get()){
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {

                if (record.value().equals("stop")){
                    consumer.pause(Collections.singletonList(topicPartition));
                }
            }

        }
    }

}
