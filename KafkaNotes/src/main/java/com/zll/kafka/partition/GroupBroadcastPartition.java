package com.zll.kafka.partition;


import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * <p>组内广播分配方案</p>
 * <p>实现了消费者组内广播,同一个消费者组内消费者可消费相同分区</p>
 */
public class GroupBroadcastPartition extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {

        Map<String, List<TopicPartition>> assigment = new HashMap<>();
        //获取所有消费者
        Set<String> consumers = subscriptions.keySet();
        for (String consumer : consumers) {
            assigment.put(consumer,new ArrayList<TopicPartition>());
        }
        //获取所有的topic
        Set<String> topics = partitionsPerTopic.keySet();

        //配置topic与consumer的对应关系
        Map<String, List<String>> consumerForTopic = getConsumerForTopic(partitionsPerTopic, subscriptions);

        //构建每个topic对应分区集
        HashMap<String, List<TopicPartition>> partitionForTopic = new HashMap<>();
        //把每一个topic的分区分给订阅该topic的所有消费者
        for (String topic : topics) {
            List<String> consumerList = consumerForTopic.get(topic);
            //构建每个topic对应分区集
            for (Integer i = 0; i < partitionsPerTopic.get(topic); i++) {
                List<TopicPartition> partitions = partitionForTopic.computeIfAbsent(topic, k -> new ArrayList<TopicPartition>());
                partitions.add(new TopicPartition(topic,i));
                partitionForTopic.put(topic,partitions);
            }

            for (String consumer : consumerList) {
                assigment.get(consumer).addAll(partitionForTopic.get(topic));
            }
        }


        return assigment;
    }

    @Override
    public String name() {
        return "GroupBroadcast";
    }

    public Map<String,List<String>> getConsumerForTopic(Map<String, Integer> partitionsPerTopic,
                                                        Map<String, Subscription> subscriptions){
        Set<String> topics = partitionsPerTopic.keySet();
        Map<String, List<String>> consumerForTopic = new HashMap<>();

        for (String topic : topics) {
            consumerForTopic.put(topic,new ArrayList<String>());
        }

        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            for (String topic : entry.getValue().topics()) {
                consumerForTopic.get(topic).add(entry.getKey());
            }
        }
        return consumerForTopic;
    }
}
