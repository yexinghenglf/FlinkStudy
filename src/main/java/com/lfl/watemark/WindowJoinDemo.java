package com.lfl.watemark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 叶星痕
 * @data 2024/5/31 下午3:01
 * 文件名 : WindowJoinDemo
 * 描述 :
 */

public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4)
                ).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                        Tuple3.of("d", 15, 1)

                ).
                assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        //笔记 window join
        DataStream<String> join = ds1.join(ds2)
                .where(r1 -> r1.f0)
                .equalTo(r2 -> r2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 联接方法，每个联接的元素对调用一次。
                     *
                     * @param first 第一个输入的元素。
                     * @param second 来自第二个输入的元素。
                     * @throws Exception 这个方法可能会抛出异常。引发异常将导致 操作失败，并可能触发恢复。
                     * @return生成的元素。
                     */
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + " <=====================> " + second;
                    }
                });

        join.print();


        env.execute();
    }
}
