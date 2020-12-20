package com.zll.kafka.delay;

import java.util.concurrent.DelayQueue;


public class RingBufferWheel {
    //时间轮桶间隔
    private Long tickMs;
    //时间轮大小，即桶的总个数
    private int wheel;
    //时间轮起始时间
    private Long startMs;
    //时间轮中任务总数量
    private Long taskCount;
    //存放任务的队列
    private DelayQueue queue;
}
