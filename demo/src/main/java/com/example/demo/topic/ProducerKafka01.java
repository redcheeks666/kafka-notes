package com.example.demo.topic;

import com.example.demo.entities.Company;
import com.example.demo.serialization.CompanySerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerKafka01 {
    public static final String brokerList="ha01:9092,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";

    public static Properties initProps(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.example.demo.serialization.CompanySerializer");

        return properties;
    }

    public static void main(String[] args) {
        //初始化配置
        Properties properties = initProps();
        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //消息实例

        ProducerRecord<String,String > record1 = new ProducerRecord<>(topic,"num01","123");
//        ProducerRecord<String, Company> record1 = new ProducerRecord<>(topic,new Company("1","zs","bj"));
        try {
            producer.send(record1).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//        producer.send(record1, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                if (exception==null){
//                    System.out.println("topic-------"+metadata.topic());
//                    System.out.println("partition-------"+metadata.partition());
//                    System.out.println("offset-------"+metadata.offset());
//                }else {
//                    exception.printStackTrace();
//                }
//            }
//        });
        //关闭连接
        producer.close();

    }
}
