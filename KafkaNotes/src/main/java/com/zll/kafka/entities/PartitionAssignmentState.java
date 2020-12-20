package com.zll.kafka.entities;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.common.Node;

@Data
@Builder
public class PartitionAssignmentState {

        private String group;
        private Node coordinator;
        private String topic;
        private int partition;
        private long offset;
        private long lag;
        private String consumerId;
        private String host;
        private String clientId;
        private long logSize;


}
