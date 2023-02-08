package com.atguigu.day02;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Example14 {
    public static void main(String[] args) {
        var queue = new ArrayBlockingQueue<Integer>(10);

        var producer = new Producer(queue);
        var consumer = new Consumer(queue);

        var t1 = new Thread(producer);
        var t2 = new Thread(consumer);

        t1.start();
        t2.start();
    }

    public static class Producer implements Runnable {
        private final BlockingQueue<Integer> queue;

        public Producer(ArrayBlockingQueue<Integer> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            for (int i = 0; i < 5; i++) {
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
                var value = queue.poll();
                if (value == null) break;
                else System.out.println(value);
            }
        }
    }
}
