package com.lfl.school;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:31
 * 文件名 : Reduce
 * 描述 :
 */
public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Tuple2<String, Integer>> list = Arrays.asList(
                Tuple2.of("hello", 3),
                Tuple2.of("flink", 2),
                Tuple2.of("hadoop", 4),
                Tuple2.of("flink", 5)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorDS = env.fromCollection(list)
                .keyBy(value -> value.f0)
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                                return Tuple2.of(
                                        value1.f0, (value1.f1 + value2.f1)
                                );
                            }

                        }
                );
        sensorDS.print();

        env.execute();
    }
}
