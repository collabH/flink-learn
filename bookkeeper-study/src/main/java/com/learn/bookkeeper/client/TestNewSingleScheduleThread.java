package com.learn.bookkeeper.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: TestNewSingleScheduleThread.java
 * @description: TestNewSingleScheduleThread.java类说明
 * @author: huangshimin
 * @date: 2023/6/7 19:36
 */
public class TestNewSingleScheduleThread {
    {
    }

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(
                new SleepThread(), 0, 30, TimeUnit.MILLISECONDS);
        while (true) {
            Thread.sleep(1000);
        }
    }

    static class SleepThread implements Runnable {

        @Override
        public void run() {
            Thread.sleep(5000);
            System.out.println(Thread.currentThread().getName() + "start...");
        }
    }
}
