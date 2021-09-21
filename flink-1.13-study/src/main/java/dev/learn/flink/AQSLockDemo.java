package dev.learn.flink;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @fileName: AQSLockDemo.java
 * @description: AQSLockDemo.java类说明
 * @author: huangshimin
 * @date: 2021/9/20 6:13 下午
 */
public class AQSLockDemo {
    private static final ReentrantLock REENTRANT_LOCK = new ReentrantLock(false);

    private static int count = 0;

    public static void main(String[] args) throws InterruptedException {
//        for (int i = 0; i < 10; i++) {
//            Thread thread = new Thread(() -> {
//                REENTRANT_LOCK.lock();
//                try {
//                    Thread.sleep(3000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                } finally {
//                    REENTRANT_LOCK.unlock();
//                }
//            });
//            thread.start();
//        }
        Condition condition = REENTRANT_LOCK.newCondition();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                REENTRANT_LOCK.lock();
                try {
                    condition.await();
                    System.out.println("test");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    REENTRANT_LOCK.unlock();
                }
            });
            thread.start();
            new Thread(()->{
                REENTRANT_LOCK.lock();
                try {
                    condition.signal();
                    System.out.println("test1");
                }catch (Exception e){

                }finally {
                    REENTRANT_LOCK.unlock();
                }
            }).start();
        }
    }
}
