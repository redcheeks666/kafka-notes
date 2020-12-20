package com.zll.kafka.reporter;


import com.zll.kafka.entities.PartitionAssignmentState;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ConsumerGroupServer {

    private String brokerList;

    private AdminClient client;

    private KafkaConsumer<String,String> kafkaConsumer;


    public ConsumerGroupServer(String brokerList){
        this.brokerList=brokerList;
    }

    //初始化方法
    public void init(){
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,brokerList);

        client = AdminClient.create(prop);

        kafkaConsumer = ConsumerGroupUtil.createNewConsumer(brokerList, "group.demo");

    }
    //释放资源
    public void close(){
        if (client!=null) client.close();
        if (kafkaConsumer!=null) kafkaConsumer.close();
    }

    /**
     * @param groupId 通过消费组id来获取对应分配分区状态
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<PartitionAssignmentState> collectGroupAssignment(String groupId)
            throws ExecutionException, InterruptedException {

        //获取消费者组List的元数据(此处只传入一个groupId)
        DescribeConsumerGroupsResult groupResult = client.describeConsumerGroups(Collections.singleton(groupId));

        //获取每个消费组内消费者的元数据
        //KafkaFuture<Map<String, ConsumerGroupDescription>>
        ConsumerGroupDescription consumersDescription = groupResult.all()
                .get().get(groupId);


        List<TopicPartition> assignedTps = new ArrayList<>();
        List<PartitionAssignmentState> rosWithConsumer = new ArrayList<>();

        //组内消费者成员元数据
        Collection<MemberDescription> members = consumersDescription.members();

        if (members!=null){
            //通过OffsetFetchRequest请求获取消费位移
            ListConsumerGroupOffsetsResult offsetResult = client.listConsumerGroupOffsets(groupId);

            //获取每个分区此时消费到的位移数据
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetResult.partitionsToOffsetAndMetadata().get();

            if (!offsets.isEmpty() && offsets!=null){
                //消费者组状态
                String state = consumersDescription.state().toString();

                if (("Stable").equals(state)){
                    rosWithConsumer = getRowsWithConsumerGroup(consumersDescription, offsets, members, assignedTps, groupId);
                }

            }

            List<PartitionAssignmentState> rowsWithOutConsumerGroup = getRowsWithOutConsumerGroup(consumersDescription, offsets, assignedTps, groupId);


            rosWithConsumer.addAll(rowsWithOutConsumerGroup);

        }

        return rosWithConsumer;
    }





    /**
     *
     * @param description 消费者组的相关数据
     * @param offsets 各个分区的偏移量元数据
     * @param members 消费者组实例集
     * @param assignedTps
     * @param group 消费者组
     * @return List<PartitionAssignmentState>
     * 有消费成员的分区数据收集方法
     */
    public List<PartitionAssignmentState> getRowsWithConsumerGroup(ConsumerGroupDescription description,
                                                              Map<TopicPartition, OffsetAndMetadata> offsets,
                                                              Collection<MemberDescription> members,
                                                              List<TopicPartition> assignedTps,
                                                              String group){

        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();

        //遍历消费者组实例集
        for (MemberDescription member : members) {
            MemberAssignment assignment = member.assignment();
            if (assignment==null){
                continue;
            }
            //获取消费者所订阅的分区集
            Set<TopicPartition> partitions = assignment.topicPartitions();


            //如果该消费者没有订阅分区
            if (partitions.isEmpty()){
                rowsWithConsumer.add(PartitionAssignmentState.builder()
                        .group(group)
                        .coordinator(description.coordinator())
                        .consumerId(member.consumerId())
                        .host(member.host())
                        .clientId(member.clientId())
                        .build()
                );
            }else {
                //如果订阅了分区则将消费信息进行收集

                //获取分区集partitions对应的HW
                Map<TopicPartition, Long> logSizes = kafkaConsumer.endOffsets(partitions);

                assignedTps.addAll(partitions);

                List<PartitionAssignmentState> pasList = partitions.stream()
                        .sorted(Comparator.comparing(TopicPartition::partition))
                        .map(partition ->
                                getConsumerAssignment(logSizes, offsets, partition, group, member, description)
                        ).collect(Collectors.toList());

                rowsWithConsumer.addAll(pasList);
            }

        }
        return rowsWithConsumer;
    }

    /**
     * 没有消费者订阅分区的状态收集
     * @param description
     * @param offsets
     * @param assignedTps
     * @param groupId
     * @return
     */
    public List<PartitionAssignmentState> getRowsWithOutConsumerGroup(ConsumerGroupDescription description,
                                                                      Map<TopicPartition,OffsetAndMetadata> offsets,
                                                                      List<TopicPartition> assignedTps,
                                                                      String groupId) {
        Set<TopicPartition> partitions = offsets.keySet();
        //assignedTps 中存储的是消费者分配到的分区(在getRowsWithConsumerGroup方法中进行的填充),即保留下的为未分配到的分区

        return partitions.stream().filter(partition -> !assignedTps.contains(partition))
                .map(partition -> {
                    long logSize = 0;
                    //获取分区HW
                    Long endoffset = kafkaConsumer.endOffsets(Collections.singleton(partition)).get(partition);

                    if (endoffset != null) logSize = endoffset;

                    //消费到的位移
                    long offset = offsets.get(partition).offset();

                    return PartitionAssignmentState.builder().group(groupId)
                            .coordinator(description.coordinator())
                            .topic(partition.topic())
                            .partition(partition.partition())
                            .logSize(logSize)
                            .lag(getLag(offset, logSize))
                            .offset(offset).build();
                }).sorted(Comparator.comparing(PartitionAssignmentState::getPartition)).collect(Collectors.toList());
    }


    /**
     *
     * @param logSizes 分区Offsets集
     * @param offsets 各个分区的偏移量
     * @param partition
     * @param group
     * @param member
     * @param description
     * @return
     */
    public PartitionAssignmentState getConsumerAssignment(Map<TopicPartition,Long> logSizes,
                                                       Map<TopicPartition,OffsetAndMetadata> offsets,
                                                       TopicPartition partition,
                                                       String group,
                                                       MemberDescription member,
                                                       ConsumerGroupDescription description){
        //获取此分区的offsetSize(HW)
        Long offsetSize = logSizes.get(partition);

        //如果当前消费者member对此分区进行了消费
        if (offsets.containsKey(partition)){
            //获取消费到的位移
            long offset = offsets.get(partition).offset();

            //计算lag
            long lag = getLag(offset, offsetSize);

            //构建当前分区对应的 PartitionAssignmentState 并返回
            return PartitionAssignmentState.builder()
                    .group(group)
                    .coordinator(description.coordinator())
                    .lag(lag)
                    .topic(partition.topic())
                    .partition(partition.partition())
                    .offset(offset)
                    .consumerId(member.consumerId())
                    .host(member.host())
                    .clientId(member.clientId())
                    .logSize(offsetSize)//HW
                    .build();
        }else {
            //如果当前消费者此时还未对此分区进行消费

            //构建PartitionAssignmentState 并返回
            return PartitionAssignmentState.builder()
                    .group(group)
                    .coordinator(description.coordinator())
                    .topic(partition.topic())
                    .partition(partition.partition())
                    .consumerId(member.consumerId())
                    .host(member.host())
                    .clientId(member.clientId())
                    .logSize(offsetSize)//HW
                    .build();
        }
    }

    //    计算Lag的方法
    private static long getLag(long offset,long logSize){
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }


     static class ConsumerGroupUtil{
        //创建Kafka ConsumerGroupUtil实例,通过KafkaConsumer.endOffsets()方法获取HW
        static KafkaConsumer<String,String> createNewConsumer(String brokerUrl,String groupId){

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerUrl);
            props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

            return new KafkaConsumer<String, String>(props);
        }


        //打印最终的输出结果的方法
        //%-ns表示左右对齐长度。
        static void printPasList(List<PartitionAssignmentState> partitionStates){
            System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s %-30s %s ",
                    "TOPIC",
                    "PARTITION",
                    "CURRENT-OFFSET",
                    "LOG-END-OFFSET",
                    "LAG",
                    "CONSUMER-ID",
                    "HOST",
                    "CLIENT-ID"
                    ));


            partitionStates.forEach(pState->{
                        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s %-30s %s ",
                                pState.getTopic(),
                                pState.getPartition(),
                                pState.getOffset(),
                                pState.getLogSize(),
                                pState.getLag(),
                                Optional.ofNullable(pState.getClientId()).orElse("-"),
                                Optional.ofNullable(pState.getHost()).orElse("-"),
                                Optional.ofNullable(pState.getClientId()).orElse("-")
                                ));
                    }
                    );

        }



    }
//    @Data
//    @Builder
//    public class PartitionAssignmentState{
//        private String group;
//        private Node coordinator;
//        private String topic;
//        private int partition;
//        private long offset;
//        private long lag;
//        private String consumerId;
//        private String host;
//        private String clientId;
//        private long logSize;
//
//    }





}
