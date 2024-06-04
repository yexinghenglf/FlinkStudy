package com.lfl.state;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @author 叶星痕
 * @data 2024/6/4 下午8:30
 * 文件名 : KeyedValueStateDemo
 * TODO : 最高的三个水位值
 */
public class KeyedListStateDemo {
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
                    ListState<Integer> VcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        VcListState = getRuntimeContext().getListState(new ListStateDescriptor<>("VcListState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        //来一条数据存转态里面
                        VcListState.add(value.getVc());

                        //从list拿出来进行排序 取前三
                        // 笔记 最好只保存三个 就是最大的前三个
                        Iterable<Integer> vcListIt = VcListState.get();
                        ArrayList<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            vcList.add(vc);
                        }

                        //降序排序
                        vcList.sort((o1, o2) -> o2 - o1);

                        //保留
                        if (vcList.size() > 3) {
                            //删除第四个
                            vcList.remove(3);
                        }
                        out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcList.toString());

                        //更新list内容
                        VcListState.update(vcList);

                    }
                });


        vcListState.print();


        env.execute();
    }
}
