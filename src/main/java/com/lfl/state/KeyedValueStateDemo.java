package com.lfl.state;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 叶星痕
 * @data 2024/6/4 下午8:30
 * 文件名 : KeyedValueStateDemo
 * TODO : 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
 */
public class KeyedValueStateDemo {
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


        DataStreamSink<String> lastVcState = sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //初始化转态
                        //状态描述 不重复的名字 类型
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcState", Types.INT));
                    }

                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //取出上一条数据的水位值
                        // 笔记 value:取值 update:更新值 clear:清空
                        int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                        //取出数据的差值的绝对值 是否超过10
                        Integer vc = value.getVc();
                        if (Math.abs(vc - lastVc) > 10) {
                            out.collect("前水位值" + vc + "后水位值" + lastVc + "相差超过10!!!!!!!!!!!!");
                        }

                        //保存更新为自己的水位值
                        lastVcState.update(vc);
                    }
                }).print();



        env.execute();
    }
}
