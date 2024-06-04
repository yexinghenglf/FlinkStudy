package com.lfl.process;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 叶星痕
 * @data 2024/6/1 下午3:31
 * 文件名 : ProcessKeyDemo
 * 描述 :
 */
public class ProcessKeyDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("idea", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(
                        Duration
                                .ofSeconds(3))
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            return element.getTs() * 1000L;
                        }

                );
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = sensorDSwithWatermark.keyBy(sensor -> sensor.getId());

        sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            /**
             * 处理输入流中的一个元素。
             *
             *＜p＞此函数可以使用｛@link Collector｝参数输出零个或多个元素，并且
             *还可以使用｛@link Context｝参数更新内部状态或设置计时器。
             *
             *@param value 输入值。
             *@param ctx 一个 ｛@link Context｝，允许查询元素的时间戳并获取
             *｛@link TimerService｝，用于注册计时器和查询时间。上下文仅
             *在调用此方法期间有效，请不要存储它。
             *@param-out返回结果值的收集器。
             *@throws Exception 这个方法可能会抛出异常。引发异常将导致
             *操作失败，并可能触发恢复。
             */
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                Long ts = ctx.timestamp();

                //定时器
                TimerService timerService = ctx.timerService();

                //注册定时器
//                timerService.registerEventTimeTimer();

            }
        });
        env.execute();
    }
}
