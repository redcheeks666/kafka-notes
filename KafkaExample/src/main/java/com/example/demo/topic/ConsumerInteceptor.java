package com.example.demo.topic;

import com.example.demo.interceptor.ConsumerInterceptorDemo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerInteceptor {
    private static final String brokerList="ha01:9092";
    private static final String groupId="group.demo";
    private static final String topic="topic-demo";
    private static final AtomicBoolean isRunning =new AtomicBoolean(true);


    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //配置消费者拦截器,不配置默认为""
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorDemo.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));


        while (isRunning.get()){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            records.forEach(x-> System.out.println(x.value()));
        }


        kafkaConsumer.close();

    }
}
