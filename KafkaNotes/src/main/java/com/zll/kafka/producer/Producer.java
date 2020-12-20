package com.zll.kafka.producer;

import com.zll.kafka.config.KafkaProperties;
import com.zll.kafka.interceptor.ProducerInterceptorDemo;
import com.zll.kafka.partition.RandomAssignorPartition;
import com.zll.kafka.serialization.CompanyDeserializer;
import com.zll.kafka.serialization.CompanySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>kafka 生产者</p>
 */
public class Producer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //自定义拦截器
        //kafkaProducer可配置多个拦截器实现链式调用,配置按逗号隔开,按配置顺序依次执行
        //拦截器执行失败,下一个拦截器接着上一个成功的拦截器继续执行.
        //properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorDemo.class.getName());
        //自定义分区器
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RandomAssignorPartition.class.getName());
        //自定义key value序列化器
        //properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());


        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=1;i<5;i++){
            ProducerRecord<String,String > record = new ProducerRecord<>(KafkaProperties.TOPIC,1,"num01"," this is a demo 这是第"+i+"条消息");


            Future<RecordMetadata> send = producer.send(record);

            //同步发送 获取元数据
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
