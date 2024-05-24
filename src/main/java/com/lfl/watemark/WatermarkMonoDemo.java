package com.lfl.watemark;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/5/24 下午4:42
 * 文件名 : WatermarkMonoDemo
 * 描述 :
 */
public class WatermarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("idea", 7777)
                .map(new WaterSensorMapFunction());
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        // 以毫秒为单位 返回的时间戳
                        System.out.println("数据时间戳 = " + element.getTs() + "毫秒数据");
                        return element.getTs() * 1000;
                    }
                });
        sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDS
                .keyBy(sensor -> sensor.getId())
                //指定窗口大小，滑动步定
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        long startTS = context.window().getStart();
                        long endTS = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口(" + windowStart + "," + windowEnd + ")包含" + count + "条数据====>" + elements.toString());
                    }
                }).print();

        env.execute();

    }
}
