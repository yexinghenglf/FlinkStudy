package com.lfl.transfrom;

import com.lfl.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @叶星痕
 * @data 2024/4/6 下午10:27
 * 文件名 : MapDemo
 * 描述 :
 */
public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

//一进多出
        sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        }).print();


        env.execute("MapDemo");
    }

    ;
}

