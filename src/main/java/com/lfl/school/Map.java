package com.lfl.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:30
 * 文件名 : Map
 * 描述 :
 */
public class Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> sensorDS = env.fromElements(1, 2, 3, 4, 5);

        SingleOutputStreamOperator<Integer> sensorMap = sensorDS.map(new MapFunction<Integer, Integer>() {

            /**
             * The mapping method. Takes an element from the input data set and transforms it into exactly
             * one element.
             *
             * @param value The input value.
             * @return The transformed value
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public Integer map(Integer value) throws Exception {

                return value * 2;
            }
        });

        sensorMap.print();
        env.execute();
    }
}
