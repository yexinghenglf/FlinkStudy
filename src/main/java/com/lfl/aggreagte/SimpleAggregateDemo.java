package com.lfl.aggreagte;

import com.lfl.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @叶星痕
 * @data 2024/4/6 下午10:27
 * 文件名 : MapDemo
 * 描述 :
 */
public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        //按照key进行分组
        KeyedStream<WaterSensor, String> keyedStream = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
//聚合算子
        SingleOutputStreamOperator<WaterSensor> result = keyedStream
//                .sum("vc");
//                .min("vc");
//                .max("vc");
        .maxBy("vc",true);


        result.print();
        env.execute("MapDemo");
    }

    ;
}

