package com.example.demo.consumerthread;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 线程封闭，为每个线程安排一个KafkaConsumer
 * 示例开启三个线程,相当于开了三个Consumer Client客户端
 * 每个线程启动,都会进行消费者再均衡.每个分区分配相应的分区.
 * 优点:每个线程对应自己分配好的分区,每个线程可以按顺序消费每个分区消息
 * 缺点:每个线程维护一个单独的KafkaConsumer,意味每个线程维护一个TCP连接,当线程数过多时，开销大.
 * */
public class EveryOneThreadConsumer {
    private static final String brokerList="ha01:9092";
    private static final String topic="topic-demo";
    private static final String groupId="group.demo";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        //开启三个线程
        int ThreadCount=3;
        for (int i = 0; i < 3; i++) {
           new KafkaThread(properties,topic).start();
        }
    }

    public static class KafkaThread extends Thread{
        private KafkaConsumer<String,String> kafkaConsumer;
        //每个线程构建独立的KafkaConsumer
        public KafkaThread(Properties properties,String topic ){
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            while (true){
                System.out.println("线程名称"+Thread.currentThread().getName());
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
//                records.forEach(x-> System.out.println("当前线程名称%s"+Thread.currentThread().getName()+"消息Value为%s"+x.value()));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("线程名称"+Thread.currentThread().getName());
                    System.out.println(record);
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
