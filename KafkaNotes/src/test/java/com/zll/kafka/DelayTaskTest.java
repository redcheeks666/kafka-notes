package com.zll.kafka;

import com.zll.kafka.delay.TaskEntry;
import com.zll.kafka.delay.TimingWheel;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.Math.max;

public class DelayTaskTest {

    public static void main(String[] args) {
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);

        TimingWheel wheel = new TimingWheel(threadPool);

        TaskEntry entry = new TaskEntry("任务执行了");
        entry.setKey(10);
        wheel.addTask(entry);
        wheel.start();

    }

}

