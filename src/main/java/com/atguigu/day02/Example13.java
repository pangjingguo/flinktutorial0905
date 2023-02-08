package com.atguigu.day02;

public class Example13 {
    public static void main(String[] args) throws Exception {
        var counter = new Counter();
        var t1 = new Thread(counter);
        var t2 = new Thread(counter);
        t1.start();
        t2.start();

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
