package com.lfl.watemark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/5/31 下午3:01
 * 文件名 : WindowJoinDemo
 * 描述 :
 */

public class IntervalJoinDemo {
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

        // 分别做keyby
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        // 调用 join
        SingleOutputStreamOperator<String> join = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 对于每对连接的元素都调用此方法。它可以输出零个或多个元素
                     * 通过提供的｛@link Collector｝，并可以访问已加入的的时间戳
                     * 元素和通过｛@link Context｝的结果。
                     *
                     * @param left  已连接对的左侧元素。
                     * @param right 连接对的右侧元素。
                     * @param ctx   允许查询左、右和联接对的时间戳的上下文。
                     *              此外，此上下文允许在侧输出上发射元素。
                     * @throws Exception 此函数可能会引发异常，导致流媒体程序
                     *                   失败后进入恢复模式。
                     * @param-out 将生成的元素发射到的收集器。
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 表示关联的数据
                        out.collect(left + " <=====================> " + right);
                    }
                });

        join.print();
        env.execute();
    }
}
