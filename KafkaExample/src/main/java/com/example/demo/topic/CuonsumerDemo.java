package com.example.demo.topic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class CuonsumerDemo {
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

        /**
         * 构建消费实例
         * KafkaConsumer中有一个partitionsFor()查询主题的元数据信息
         *    List<PartitionInfo> partitionsFor(String topic);
         *    List<PartitionInfo> partitionsFor (String topic, Duration timeout);
         *
         * */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
//        consumer.partitionsFor()
        /**
         * 订阅方式:(三种方式互斥,一个消费者只能使用其中一种)
         *     @topics 主题集合
         *     void subscribe(Collection<String> topics);
         *
         *     @callback 设置相应的在均衡监听器
         *     void subscribe (Collection < String > topics, ConsumerRebalanceListener callback);
         *
         *     @pattern 规则订阅 以正则表达式订阅 如 consumer.subscribe(Pattern.compile("topic_.*"));
         *     void subscribe (Pattern pattern);
         *
         *     @callback 设置相应的在均衡监听器
         *     void subscribe (Pattern pattern, ConsumerRebalanceListener callback);
         *
         *     @partitions 指定分区集合 TopicPartition { topic partition }
         *     如consumer.assign(Arrays.asList(new TopicPartition("topic-demo",0)));
         *     只订阅topic-demo主题的0分区消息
         *     void assign (Collection < TopicPartition > partitions);
         * */
//        consumer.subscribe(Pattern.compile("topic-.*"));

        //        分区集
        List<TopicPartition> partitions = new ArrayList<>();
        //        获取当前主题分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        //        订阅当前主题的全部分区消息
        if (partitionInfos!=null){
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
            }
        }
        /**
         * 取消订阅:(三个方法实现效果相同)
         *  consumer.unsubscribe();
         *  consumer.subscribe(new ArrayList<String>());
         *  consumer.assign(new ArrayList<TopicPartition>());
         * */
        consumer.assign(partitions);

//        consumer.assign(Arrays.asList(new TopicPartition("topic-demo",0)));
        //循环消费
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            //消息总数
            int count = consumerRecords.count();
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.value());
                System.out.println(record.topic());
                System.out.println(record.offset());
                System.out.println(record.partition());
                System.out.println(record.timestamp());
                System.out.println(record.key());
            }
        }

    }
}
