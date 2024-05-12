package com.lfl.transfrom;

import com.lfl.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @叶星痕
 * @data 2024/4/6 下午10:27
 * 文件名 : MapDemo
 * 描述 :
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
//        匿名内部类
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//
//        });
//        map.print();


//        //lambda表达式
//        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());
//        map.print();

        //方式三:定义一个类实现MapFunction接口
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapDemo().new MyMapFunction());
        map.print();

        env.execute("MapDemo");
    }
    public class MyMapFunction implements MapFunction<WaterSensor,String>{

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
