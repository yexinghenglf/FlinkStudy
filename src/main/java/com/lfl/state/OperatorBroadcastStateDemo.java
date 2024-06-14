package com.lfl.state;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/6/5 下午5:08
 * 文件名 : OperatorBroadcastStateDemo
 * 描述 :
 */

public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {

        //TODO 水位超过指定的阈值发送告警，阈值可以动态修改。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("idea", 7777)
                .map(new WaterSensorMapFunction());

        //广播流
        DataStreamSource<String> thresholdDS = env.socketTextStream("idea", 8888);

        //配置广播流
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> broadcastBS = thresholdDS.broadcast(broadcastMapState);

        //把数据流和广播流 connect 起来
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(broadcastBS);

        //调用process
        SingleOutputStreamOperator<String> process = sensorBCS.process(
                new BroadcastProcessFunction<WaterSensor, String, String>() {
                    //TODO 数据流的处理方法
                    @Override
                    public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //TODO 通过上下文获取广播状态 取出值， 只能读不能修改
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        Integer threshold = broadcastState.get("threshold");

                        //判断广播里是否有数据
                        threshold = threshold == null ? 0 : threshold;
                        if (value.getVc() > threshold) {
                            out.collect("传感器id为" + value.getId() + ",水位值为" + value.getVc() + "超过阈值" + threshold);
                        }
                    }

                    //TODO 广播后的配置流的处理方法
                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                        //TODO 通过上下文获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        broadcastState.put("threshold", Integer.valueOf(value));
                    }
                }
        );
        process.print();

        env.execute();
    }
}
