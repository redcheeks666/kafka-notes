package com.zll.kafka.delay;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 每一个时间轮的桶即一个TimerTaskList链表
 * TimerTaskList封装若干个task存入DelayQueue,所以需要实现Delayed
 */
public  class TimerTaskList  implements Delayed {


    /**
     *  判断是否到期取出
     * @param unit
     * @return
     */
    @Override
    public long getDelay(TimeUnit unit) {
        return 0;
    }

    /**
     *  根据过期时间进行队列排序,过期时间短的会被排在队列前端
     * @param o
     * @return
     */
    @Override
    public int compareTo(Delayed o) {
        return 0;
    }
}
//
// class TimeTaskEntry{
//
//
//}
