package com.zll.kafka;

import com.zll.kafka.entities.PartitionAssignmentState;
import com.zll.kafka.reporter.ConsumerGroupServer;
import com.zll.kafka.util.ConsumerGroupUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class JmxConnectionTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ConsumerGroupServer server = new ConsumerGroupServer("ha01:9092");

        server.init();

        List<PartitionAssignmentState> partitionAssignmentStates = server.collectGroupAssignment("group.demo");

        ConsumerGroupUtil.printPasList(partitionAssignmentStates);

    }

}

