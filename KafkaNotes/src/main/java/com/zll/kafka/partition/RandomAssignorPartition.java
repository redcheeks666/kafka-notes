package com.zll.kafka.partition;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * <p>自定义分区器</p>
 * <P>实现分区对消费者的随机分配</P>
 */
public class RandomAssignorPartition extends AbstractPartitionAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {


        HashMap<String, List<TopicPartition>> assignment = new HashMap<>();
        //获取组内所有的消费者
        Set<String> consumers = subscriptions.keySet();

        for (String consumer : consumers) {
            assignment.put(consumer,new ArrayList<TopicPartition>());
        }

        //针对每一个topic进行分区分配
        Set<TopicPartition> partitions = new HashSet<>();
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            //获取订阅的所有topic
            List<String> topics = entry.getValue().topics();

            if (topics==null) continue;


            for (String topic : topics) {
                Integer partnum = partitionsPerTopic.get(topic);
                if (partnum==null) continue;
                for (Integer i = 0; i < partnum; i++) {
                    partitions.add(new TopicPartition(topic,i));
                }
            }
        }
        //获取分区与消费者订阅关系映射
        Map<TopicPartition, List<String>> counsumerForPartition = getCounsumerForPartition(partitionsPerTopic, subscriptions);

        //partitions所有分区
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();

            //获取订阅该分区的消费者集
            List<String> consumerlist = counsumerForPartition.get(partition);

            if (consumerlist.size()>0){
                //随机分到消费者
                int i = new Random().nextInt(consumerlist.size());
                String consumer = consumerlist.get(i);
                //添加到分配方案中
                assignment.get(consumer).add(partition);
            }


        }

        return assignment;
    }

    @Override
    public void onAssignment(Assignment assignment, int generation) {

    }

    @Override
    public String name() {
        return "RandomAssignor";
    }

    //获取分区与订阅消费者的映射
    public Map<TopicPartition,List<String>> getCounsumerForPartition(Map<String, Integer> partitionsPerTopic,
                                                                     Map<String, Subscription> subscriptions){
        HashMap<TopicPartition, List<String>> consumerForPartition= new HashMap<>();
        //构建分区与消费者的对应关系
        for (Map.Entry<String, Integer> entry : partitionsPerTopic.entrySet()) {
            for (Integer i = 0; i < entry.getValue(); i++) {
                consumerForPartition.put(new TopicPartition(entry.getKey(),i),new ArrayList<>());
            }
        }
//        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
////
////        }
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            entry.getValue().topics().stream().filter(topic->partitionsPerTopic.get(topic)!=null).forEach(
                topic->{
                    for (Integer i = 0; i < partitionsPerTopic.get(topic); i++) {
                        TopicPartition topicPartition = new TopicPartition(topic, i);
                        consumerForPartition.get(topicPartition).add(entry.getKey());
                    }
                }
            );
        }
        return consumerForPartition;
    }
}
