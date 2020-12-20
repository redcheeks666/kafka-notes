package com.zll.kafka.consumer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * <p>kafka 消费者</p>
 *
 *
 * <p>订阅方式:(三种方式互斥,一个消费者只能使用其中一种)</p>
 * {@topics 主题集合}
 *  void subscribe(Collection<String> topics);
 *
 * {@callback 设置相应的在均衡监听器}
 *  void subscribe (Collection < String > topics, ConsumerRebalanceListener callback);
 *
 * {@pattern 规则订阅 以正则表达式订阅 如 consumer.subscribe(Pattern.compile("topic_.*"));}
 *  void subscribe (Pattern pattern);
 *
 * {@callback 设置相应的在均衡监听器}
 *  void subscribe (Pattern pattern, ConsumerRebalanceListener callback);
 *
 * {@partitions 指定分区集合 TopicPartition(topicName partitionNum) }
 *  如consumer.assign(Arrays.asList(new TopicPartition("topic-demo",0)));
 *
 * {订阅期间,新加入消费者不支持消费者重平衡}
 *  void assign (Collection < TopicPartition > partitions);
 *
 *
 * <p>取消订阅:(三个方法实现效果相同)</p>
 *  consumer.unsubscribe();
 *  consumer.subscribe(new ArrayList<String>());
 *  consumer.assign(new ArrayList<TopicPartition>());
 *
 * <p>消费位移手动提交</p>
 * 同步提交
 * void commitSync();
 * void commitSync(Duration timeout);
 * void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
 * void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
 * 异步提交
 * <p>
 *     对于异步提交。提交失败后的重试会造成重复消费,比如第一次提交位移100失败了
 *     第二次提交150成功了，此时第一次开始重试，提交n成功了，位移就会变为100，消息重复消费。
 *     解决方法:
 *     增加事务，使用单调递增的序号，每次异步提交，增加序号相应的值，
 *     每次提交成功后，对比当前提交位移与序号值，位移<自增序号，
 *     已有最新的位移提交，无需重试，位移=自增序号，可重试。
 * </p>
 * void commitAsync();
 * void commitAsync(OffsetCommitCallback callback);
 * void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);
 *
 */
public class Cuonsumer {



    public static Properties initProp(){

        Properties properties = new Properties();
        //客户端id 默认"",如果不设置,kafkaconsumer自动分配形式如"consumer-1"字符串
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,KafkaProperties.CLIENT_ID);
        //消费者组id,每个消费者都隶属于一个消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,KafkaProperties.GROUP_ID);
        //手动提交位移
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //配置消费者拦截器,不配置默认为""
//        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorDemo.class.getName());

        return properties;
    }


    public static void main(String[] args) {

        Properties properties = initProp();



        //构建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //订阅主题
        //consumer.subscribe(Pattern.compile("topic-.*"));
        consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));

        //分区粒度提交位移
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(1000);

            //Get just the records for the given partition
            //只获取指定分区的数据
            //List<ConsumerRecord<String, String>> partitionRecord = records.records(new TopicPartition(KafkaProperties.TOPIC,1));

            consumer.assignment();
            //当前收到消息的所有TopicPartition{topic,partition}
            Set<TopicPartition> tps = records.partitions();

            //每一个主题的每一个分区
            for (TopicPartition tp : tps) {
                //当前主题分区的所有消息
                List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

                for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                    System.out.println(partitionRecord.value());
                }
                //当前消息集中的最大位移
                long lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();



                //对每个分区精准提交提交上面的位移
                //conseumer.commitSync(Collections.singletonMap(tp,new OffsetAndMetadata(lastConsumerOffset+1)));
                try {
                    //异步提交
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

                            if (exception==null) {
                                Set<TopicPartition> topicPartitions = offsets.keySet();
                                topicPartitions.forEach(x-> System.out.println(offsets.get(x)));
                            }else {
                                System.out.println("提交位移失败");
                                //退出循环 KafkaConsumer唯一可以从其他线程安全调用方法
                                consumer.wakeup();
                            }
                        }
                    });

                    consumer.close();
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    //提交失败时 再次重试提交
                    consumer.commitSync();

                }

            }



        }


    }
}
