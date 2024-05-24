package com.lfl.window;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;


/**
 * @author 叶星痕
 * @data 2024/5/20 下午2:43
 * 文件名 : WindowReduceDemo
 * 描述 :
 */
public class windowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));


        SingleOutputStreamOperator<String> process = sensorWS
//                .apply(
//                        /**
//                         * <IN> – 输入值的类型。
//                         * <OUT> – 输出值的类型。
//                         * <KEY> – 密钥的类型。
//                         * <W> – 可以应用此窗口函数的窗口类型。
//                         */
//                        new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//                            /**
//                             *
//                             * @param s 用于计算此窗口的键。
//                             *@param window 正在评估的窗口。
//                             *@param input 正在评估的窗口中的元素。
//                             *@param out 发射元件的收集器。
//                             *@throws 异常
//                             */
//                            @Override
//                            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
//
//                            }
//                        })
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            /**
                             * Evaluates the window and outputs none or several elements.
                             * *@params 用于计算此窗口的键。
                             *
                             * @param context  正在评估窗口的上下文。
                             * @param elements 正在评估的窗口中的元素。
                             * @param out      发射元件的收集器。
                             * @throws Exception 函数可能会抛出异常使程序失败并触发恢复。
                             */
                            @Override
                            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                // NOTE 上下文可以拿到window对象
                                long startTS = context.window().getStart();
                                long endTS = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("key=" + s + "的窗口(" + windowStart + "," + windowEnd + ")包含" + count + "条数据====>" + elements.toString());

                            }
                        }
                );
        process.print();

        env.execute();
    }
}
