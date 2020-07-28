package com.example.demo.topic;

import com.example.demo.interceptor.ProducerInterceptorDemo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerInterceptor {
    public static final String brokerList="ha01:9092,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";
    /**
     * 初始化配置
     * */
    public static Properties initProp(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        /**
         * kafkaProducer可配置多个拦截器实现链式调用,配置按逗号隔开,按配置顺序依次执行
         * 拦截器执行失败,下一个拦截器接着上一个成功的拦截器继续执行.
         * */
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ProducerInterceptorDemo.class.getName());
        return prop;
    }
    public static void main(String[] args) {
        //初始化
        Properties prop = initProp();
        //构建producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);
        //消息实例构建
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "demo");
        //发送
        producer.send(record);
        //关闭连接
        producer.close();
    }
}
