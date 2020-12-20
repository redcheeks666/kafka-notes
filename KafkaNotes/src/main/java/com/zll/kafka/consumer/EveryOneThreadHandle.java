package com.zll.kafka.consumer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>多线程消费单Consumer实例</p>
 * <p>
 *     此方式实现思想为单消费实例，打破消费速度瓶颈
 *     扩展性强，但实现难度高，易造成消息丢失，难以维护消费顺序。
 * </p>
 */
public class EveryOneThreadHandle {
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    private static final Map<TopicPartition, OffsetAndMetadata> offsets=new HashMap<>();

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        //手动提交位移
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }
    //此处可横向扩展  开启多个kafkaConsumerThread 来进一步提升消费
    /**for (int i = 0; i < 5; i++) {
     *   new KafkaConsumerThread().start();
     *   }
     */
    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, KafkaProperties.TOPIC, Runtime.getRuntime().availableProcessors());

        kafkaConsumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread{
        private KafkaConsumer<String,String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNum;


        public KafkaConsumerThread(Properties properties,String topic ,int threadNum) {
            KafkaConsumer<String, String> KafkaConsumer = new KafkaConsumer<>(properties);

            kafkaConsumer.subscribe(Arrays.asList(topic));
            this.threadNum=threadNum;

             //@Param corePoolSize:threadNum 核心线程数量
             //@Param maximumPoolSize:threadNum 最大线程数量
             //@Param keepAliveTime:0L 表示空闲线程的存活时间
             //@Param unit:TimeUnit.MINUTES keepAliveTime的单位
            executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        }

        @Override
        public void run() {

            try {
                while (isRunning.get()){
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()){
                        executorService.submit(new RecordHandle(records));

                        synchronized (offsets){
                            if (!offsets.isEmpty()){
                                kafkaConsumer.commitSync(offsets);
                                offsets.clear();
                            }
                        }

                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }

        }
    }

    public static class RecordHandle extends Thread{

        public final ConsumerRecords<String, String> records;


        public RecordHandle(ConsumerRecords<String, String> records){
            this.records=records;
        }


        @Override
        public void run(){

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> consumerRecordList = this.records.records(partition);
                //获取最后一条位移
                long lastOffset = consumerRecordList.get(consumerRecordList.size() - 1).offset();

                synchronized (offsets){

                    //在这里,将每个分区的位移缓存在offsets中共享。在poll方法后取出提交。
                    if (!offsets.containsKey(partition)){
                        offsets.put(partition,new OffsetAndMetadata(lastOffset+1));
                    }else {
                        long partitionOffset = offsets.get(partition).offset();
                        //如果共享变量中的位移 小于当前消费的位移 则更新共享的位移(正常位移顺序消费)
                        //反之，则不对共享位移进行操作，此时消费的位移属于共享位移之前的消息。
                        if (partitionOffset < lastOffset+1){
                            offsets.put(partition,new OffsetAndMetadata(lastOffset+1));
                        }
                    }

                }


            }
            //处理消息
            System.out.println(records);
        }


    }



}
