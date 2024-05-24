package com.lfl.school;

import com.lfl.split.SplitByFilterDemo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/5/17 上午9:31
 * 文件名 : Window
 * 描述 :
 */
public class Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> datasource = env
                .socketTextStream("idea", 7777)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        datasource.print();

        env.execute();

        env.execute();
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and
         * transforms it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *                   operation to fail and may trigger recovery.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String s : value.split(" ")) {
                out.collect(new Tuple2<String, Integer>(s,1));
            }
        }
    }
}
