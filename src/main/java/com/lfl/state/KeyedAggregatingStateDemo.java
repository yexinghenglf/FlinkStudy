package com.lfl.state;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 叶星痕
 * @data 2024/6/4 下午8:30
 * 文件名 : KeyedValueStateDemo
 * TODO : 计算每种传感器的平均水位
 */
public class KeyedAggregatingStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("idea", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (elements, ts) -> elements.getTs() * 1000L
                                )
                );


        SingleOutputStreamOperator<String> vcListState = sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            AggregatingState<Integer, Double> vcAvgAggregatingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcAvgAggregatingState = getRuntimeContext()
                                        .getAggregatingState(
                                                new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                                        "vcAvgAggregatingState",
                                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                            @Override
                                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                                return Tuple2.of(0, 0);
                                                            }

                                                            @Override
                                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                                            }

                                                            @Override
                                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                                return (double) (accumulator.f0 / accumulator.f1);
                                                            }

                                                            @Override
                                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                                return null;
                                                            }
                                                        },
                                                        Types.TUPLE(Types.INT, Types.INT)
                                                )
                                        );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                                // 将水位值添加到聚合转态中
                                vcAvgAggregatingState.add(value.getVc());

                                //获取结果
                                Double vcAvg = vcAvgAggregatingState.get();

                                out.collect("传感器id：" + value.getId() + "，平均水位：" + vcAvg);
                            }
                        }
                );


        vcListState.print();


        env.execute();
    }
}
