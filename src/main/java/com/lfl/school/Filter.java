package com.lfl.school;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:30
 * 文件名 : Filter
 * 描述 :
 */
public class Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source = env.fromElements(5, 4, 6, 7, 2, 1, 63, 7, 5, 8);
        SingleOutputStreamOperator<Integer> filter = source.filter(value -> value > 0);
        filter.print();
        env.execute();
    }
}
