package com.atguigu.day03;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Example5 {
    public static void main(String[] args) throws Exception {
        // 使用数组作为阻塞队列，队列的大小是10个元素
        var queue = new ArrayBlockingQueue<Integer>(10);

        var producer = new Producer(queue);
        var consumer = new Consumer(queue);

        var t1 = new Thread(producer);
        var t2 = new Thread(consumer);

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }

    public static class Producer implements Runnable {
        private final BlockingQueue<Integer> queue;

        public Producer(BlockingQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 100; i++) {
                // 向队列中发送数据
                // 线程安全的写入，原子性的添加
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                queue.add(i);
            }
        }
    }

    public static class Consumer implements Runnable {
        private final BlockingQueue<Integer> queue;

        public Consumer(BlockingQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // 从队列中消费数据
                var value = queue.poll();
                if (value != null) System.out.println("消费到" + value);
            }
        }
    }
}
