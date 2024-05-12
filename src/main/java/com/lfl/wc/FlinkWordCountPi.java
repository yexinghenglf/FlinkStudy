package com.lfl.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author 叶星痕
 * @data 2024/4/26 上午8:44
 * 文件名 : FlinkWordCountPi
 * 描述 :
 */
public class FlinkWordCountPi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("input/word.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> map = source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
//        UnsortedGrouping<Tuple2<String, Long>> wordcount = map.groupBy(0);

        env.execute();
    }
}
