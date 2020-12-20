package com.zll.kafka.config;


public class KafkaProperties {
    public static final String TOPIC = "topic-demo";
    public static final String KAFKA_SERVER_URL_PORT = "ha01:9092";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String GROUP_ID = "group.demo";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";


    private KafkaProperties() {}

}
