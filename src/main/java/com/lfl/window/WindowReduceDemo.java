package com.lfl.window;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
 * @author 叶星痕
 * @data 2024/5/20 下午2:43
 * 文件名 : WindowReduceDemo
 * 描述 :
 */
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1.窗口分配器

        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //2.窗口函数
        SingleOutputStreamOperator<WaterSensor> sensorRS = sensorWS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
                }
        );
        sensorRS.print();
        env.execute();
    }
}
