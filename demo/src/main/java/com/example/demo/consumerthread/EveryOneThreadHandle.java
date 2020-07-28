package com.example.demo.consumerthread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class EveryOneThreadHandle {
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

    }

    public static class KafkaConsumerThread{
        private KafkaConsumer<String,String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNum;


        public KafkaConsumerThread(Properties properties,String topic ,int threadNum) {
            KafkaConsumer<String, String> KafkaConsumer = new KafkaConsumer<>(properties);

            kafkaConsumer.subscribe(Arrays.asList(topic));
            this.threadNum=threadNum;
            /**
             * @Param corePoolSize:threadNum 核心线程数量
             * @Param maximumPoolSize:threadNum 最大线程数量
             * @Param keepAliveTime:0L 表示空闲线程的存活时间
             * @Param unit:TimeUnit.MINUTES keepAliveTime的单位
             *
             * */
            executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        }
    }



}
