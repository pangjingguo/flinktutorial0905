package com.atguigu.day03;

public class Example4 {
    public static void main(String[] args) throws Exception {
        Counter counter = new Counter();

        var t1 = new Thread(counter);
        var t2 = new Thread(counter);

        // .start方法会自动执行Counter中的run方法
        t1.start();
        t2.start();

        // 保证两个线程都执行完
        t1.join();
        t2.join();
    }

    public static class Counter implements Runnable {
        private int counter = 0;
        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                synchronized (this) {
                    counter++;
                }
            }
            System.out.println(counter);
        }
    }
}
