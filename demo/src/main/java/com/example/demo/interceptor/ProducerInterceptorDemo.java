package com.example.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerInterceptorDemo implements ProducerInterceptor<String,String> {

     /**
      * AtomicInteger() 变量省去synchronized修饰,且保证原子性,线程安全的加减.
      * AtomicInteger() 使用非阻塞算法控制并发.
      */
      AtomicInteger success = new AtomicInteger();
      AtomicInteger failed  = new AtomicInteger();



    /**
     * onsend操作消息,对消息定制化操作
     * 尽量不修改消息元数据,如topic key partition等信息
     * 示例通过onSend为每个消息的value增加前缀"prefix-interceptor"
     * */
    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord<String,String> record) {
        String value = "prefix-interceptor"+record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),record.key(),value,record.headers());
    }

    /**
     * 消息应答之前,或者消息发送失败调用.
     * 优先于producer的Callback执行.
     * 方法运行在kafkaProducer的IO线程中,逻辑尽量简单,避免影响发送速度.
     * 示例中统计消息发送成功失败次数
     * @param metadata 与exception互斥 一方有值,另一个为null
     * @param exception
     * */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null==exception){//消息发送成功
            success.getAndIncrement();
        }else {//失败
            failed.getAndIncrement();
        }
    }
    /**
     * 关闭拦截器时,清理一些资源.
     * 示例种打印消息发送成功几率
     * */
    @Override
    public void close() {
        double i = (double) success.get() / (success.get() + failed.get());
        System.out.println("消息发送成功几率"+i*100+"%");
    }
    /**
     * ProducerInterceptor与Partitioner接口有共同的父接口Configurable
     * Configurable接口的方法
     * */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
