package com.example.demo;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class demotest {



    private static final ReentrantLock lock = new ReentrantLock();
    private static final Condition condition = lock.newCondition();

    private static boolean flag = true;

    public static void main(String[] args) throws Exception {
        Thread waitThread = new Thread(new Waiter(), "Wait Thread");
        waitThread.start();
        Thread.sleep(1000);
        Thread notifyThread = new Thread(new Notifier(), "Notify Thread");
        notifyThread.start();
    }

    private static class Waiter implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                while (flag) {
                    System.out.println(Thread.currentThread() + " flag is true. wait @ " +
                            new SimpleDateFormat("HH:mm:ss").format(new Date()));
                    System.out.println(1);
                    condition.await();
                }
                System.out.println(Thread.currentThread() + " flag is false. running @ " +
                        new SimpleDateFormat("HH:mm:ss").format(new Date()));
                System.out.println(2);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            } finally {
                lock.unlock();
            }
        }
    }

    private static class Notifier implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                System.out.println(Thread.currentThread() + " hold lock. notify @ " +
                        new SimpleDateFormat("HH:mm:ss").format(new Date()));
                System.out.println(3);
                condition.signalAll();
                System.out.println(4);
                flag = false;
                Thread.sleep(5000); // Thread.sleep() 不会释放锁

                System.out.println(Thread.currentThread() + " hold lock again. sleep @ " +
                        new SimpleDateFormat("HH:mm:ss").format(new Date()));
                System.out.println(5);
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            } finally {
                lock.unlock();
            }

        }
    }



}

