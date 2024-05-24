package com.lfl.school;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:30
 * 文件名 : FlatMAp
 * 描述 :
 */
public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements("Hello World", "Hello Java", "Hello Hadoop", "Hello Python");

        SingleOutputStreamOperator<String> soureceFlatMap = source.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) {
                            out.collect(s);
                        }
                    }
                }
        );

        soureceFlatMap.print();
        env.execute();
    }
}
