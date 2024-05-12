package com.lfl.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/4/13 下午3:39
 * 文件名 : UnioDemo
 * 描述 :
 */
public class UnioDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<String> source3 = env.fromElements("1111", "2222", "3333", "4444", "5555");

        source1.union(source2).print();

        env.execute();
    }
}
