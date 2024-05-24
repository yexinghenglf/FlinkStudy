package com.lfl.school;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:30
 * 文件名 : KeyBy
 * 描述 :
 */
public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Tuple2<String, Integer>> list = Arrays.asList(
                Tuple2.of("hello", 3),
                Tuple2.of("flink", 2),
                Tuple2.of("hadoop", 4),
                Tuple2.of("flink", 5));

        DataStreamSource<Tuple2<String, Integer>> datasource = env.fromCollection(list);

        KeyedStream<Tuple2<String, Integer>, String> keyBy = datasource.keyBy(
                value -> value.f0
        );

        keyBy.print();
        env.execute();
    }
}
