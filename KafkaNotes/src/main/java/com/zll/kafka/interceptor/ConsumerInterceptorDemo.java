package com.zll.kafka.interceptor;


import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import sun.rmi.runtime.Log;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author zll
 * @date 2020-02-05
 *
 * 消费者拦截器
 *
 * 使用消费者拦截器，实现消息过期时间TTL
 * 设置超过系统时间戳10秒的消息不予消费
 */
public class ConsumerInterceptorDemo implements ConsumerInterceptor<String,String> {


    private static final long ttl_max_time=10*1000L;

    /**
     * @param records
     * @return
     *
     * 此方法在poll()方法返回之前调用，对返回的数据进行操作,onConsume方法异常不会向上抛,会记录在日志
     */
    @Override
    public ConsumerRecords<String,String> onConsume(ConsumerRecords<String,String> records) {
        long currentTimeMillis = System.currentTimeMillis();
        Set<TopicPartition> partitions = records.partitions();
        Map<TopicPartition,List<ConsumerRecord<String, String>>> filterrecords = new HashMap<>();

        for (TopicPartition partition : partitions) {
            List<ConsumerRecord<String, String>> rs = records.records(partition);
            //过滤超时消息
            List<ConsumerRecord<String, String>> recordList = rs.stream()
                    .filter(x -> x.timestamp() - currentTimeMillis < ttl_max_time)
                    .collect(Collectors.toList());
            filterrecords.put(partition,recordList);
        }
        for (ConsumerRecord<String, String> record : records) {
                record.timestamp();
        }

        System.out.println(">>>>>>>>>>>>>>>拦截器安排上了>>>>>>>>>>>>>");
        return new ConsumerRecords<>(filterrecords);
    }

    @Override
    public void close() {

    }

    /**
     *消费者提交完消费位移之后调用,可以记录跟踪位移提交信息
     * */
    @Override
    public void onCommit(Map offsets) {
        offsets.forEach((partition,offset)->{

            System.out.printf("分区%s的位移为>>>>>>>>>%s",partition,offset);

        });
    }

    /**
     * 配置方法
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
