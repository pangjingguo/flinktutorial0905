package com.atguigu.day02;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 有向无环图
public class Example1 {
    public static void main(String[] args) {
        var dag = new HashMap<String, List<String>>();

        // 添加A --> B, A --> C
        var ANeighbors = new ArrayList<String>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);

        // 添加B --> D, B --> E
        var BNeighbors = new ArrayList<String>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        BNeighbors.add("C");
        dag.put("B", BNeighbors);

        var CNeighbors = new ArrayList<String>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        // 拓扑排序
        // 第一个参数：有向无环图
        // 第二个参数：当前遍历的顶点
        // 第三个参数：已经找到的路径
        topologicalSort(dag, "A", "A");
    }

    // topologicalSort(dag, "A", "A");
    // topologicalSort(dag, "B", "A --> B");
    // topologicalSort(dag, "D", "A --> B --> D");
    public static void topologicalSort(Map<String, List<String>> dag, String vertex, String path) {
        // 如果当前遍历的顶点是终点，那么直接打印输出已经找到的路径
        if (vertex.equals("D") || vertex.equals("E")) {
            System.out.println(path);
        }
        // 如果当前遍历的顶点不是终点，将当前遍历的顶点添加到路径path中，并继续遍历当前顶点指向的所有顶点
        else {
            for (var neighbor : dag.get(vertex)) {
                topologicalSort(dag, neighbor, path + " --> " + neighbor);
            }
        }
    }
}
