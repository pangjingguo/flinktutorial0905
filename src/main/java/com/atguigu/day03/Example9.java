package com.atguigu.day03;

public class Example9 {
    public static void main(String[] args) {
        var obj1 = Singleton.getInstance();
        var obj2 = Singleton.getInstance();
    }

    public static class Singleton {
        private static Singleton instance = null;

        private Singleton() {
        }

        public static Singleton getInstance() {
            if (instance == null) {
                instance = new Singleton();
            }
            return instance;
        }
    }
}
