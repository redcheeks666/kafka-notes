package com.example.demo.topic;

import com.example.demo.interceptor.ProducerInterceptorDemo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerKafka {
    public static final String brokerList="ha01:9092,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
      //  properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorDemo.class.getName());
        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //消息实例
        ArrayList<Object> objects = new ArrayList<>();

        for (int i=1;i<5;i++){
            ProducerRecord<String,String > record = new ProducerRecord<>(topic,1,"num01"," this is a demo 这是第"+i+"条消息");

            Future<RecordMetadata> send = producer.send(record);
            try {
                RecordMetadata recordMetadata = send.get();
                long timestamp = recordMetadata.timestamp();
                System.out.println(recordMetadata.partition());
                System.out.println(recordMetadata.topic());
                System.out.println(timestamp);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        //关闭连接
        producer.close();
    }
}
