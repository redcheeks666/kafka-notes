package com.zll.kafka.topic;


import com.zll.kafka.config.KafkaProperties;
import jdk.nashorn.internal.runtime.regexp.joni.ast.Node;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CreateTopic {

    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        AdminClient adminClient = KafkaAdminClient.create(properties);
        //TopicName PartitionNum ReplicaNum
        NewTopic topic01 = new NewTopic("a-topic", 1, (short) 3);
        adminClient.createTopics(Arrays.asList(topic01));

        ListTopicsResult result = adminClient.listTopics();

        try {
            for (TopicListing topicListing : result.listings().get()) {
                System.out.println(topicListing.name());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }



    }
}
