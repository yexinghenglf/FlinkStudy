package com.lfl.split;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO DataStream实现Wordcount：读socket（无界流）
 *
 * @author cjp
 * @version 1.0
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("8.149.139.255", 7777)
                .map(new WaterSensorMapFunction());

        // TODO 2. 处理数据 把s1 和 s2 分别输出到不同的侧输出流
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.id;
                if ("s1".equals(id)) {
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(id)) {
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        process.print("非侧输出流：");
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        s1.print();
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);
        s2.print();
        env.execute();
    }
}

