package com.lfl.state;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * @author 叶星痕
 * @data 2024/6/4 下午8:30
 * 文件名 : KeyedValueStateDemo
 * TODO : 统计每种传感器每种水位值出现的次数
 */
public class KeyedMapStateDemo {
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    MapState<Integer, Integer> vcCountMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcCountMap = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountMap", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断是否存在对应的vc的key
                        Integer vc = value.getVc();
                        if (vcCountMap.contains(vc)) {
                            //如果包含 vc 则+1
                            Integer count = vcCountMap.get(vc);
                            vcCountMap.put(vc, ++count);
                        } else {
                            //如果不包含vc 则设置为1
                            vcCountMap.put(vc, 1);
                        }
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("传感器id为" + value.getId() + "\n");
                        for (Map.Entry<Integer, Integer> vcCount : vcCountMap.entries()) {
                            outStr.append(vcCount.getKey() + " : " + vcCount.getValue() + "\n");
                        }
                        outStr.append("=====================================");
                        out.collect(outStr.toString());
                    }
                });


        vcListState.print();

//        Ye Stardust
        env.execute();
    }
}
