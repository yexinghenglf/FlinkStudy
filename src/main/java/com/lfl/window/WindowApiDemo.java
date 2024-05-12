package com.lfl.window;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/4/22 下午2:51
 * 文件名 : WindowApi
 * 描述 :
 */
public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("120.26.10.18", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //基于时间的滚动窗口
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));//基于时间

        //基于时间的滑动窗口 第一个参数是窗口大小，第二个参数是滑动步长
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        //基于时间的会话窗口 会话窗口是指在规定时间内没有数据到来就会关闭窗口
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //基于计数的
//        sensorKS.countWindow(5);//每5条数据开一个窗口
        //基于计数的滑动窗口
//        sensorKS.countWindow(5,2);//每5条数据开一个窗口，每2条数据滑动一次
        //全局窗口
//        sensorKS.window(GlobalWindows.create());//全局窗口(不分组，所有数据都放在一个窗口中)

//增量聚合：来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        sensorKS
//                .reduce()
//                .aggregate()
//全窗口函数： 数据来了不计算，存起来，窗囗触发的时候，计算并输出结果
//        sensorKS.process()

        env.execute();

    }
}
