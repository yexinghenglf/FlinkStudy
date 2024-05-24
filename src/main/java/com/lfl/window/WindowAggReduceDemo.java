package com.lfl.window;

import com.lfl.bean.WaterSensor;
import com.lfl.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
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
public class WindowAggReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // 1.窗口分配器

        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS
                .window(
                        TumblingProcessingTimeWindows
                        .of(Time.seconds(5))
                );

        //2.Aggregate窗口函数

        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                /**
                 * 第一个是输入的类型
                 * 第二个是累加器的类型 也就是中间存储的类型
                 * 第三个是输出的类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法");
                        return 0;
                    }

                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("调用getResult方法");
                        return accumulator.toString();
                    }

                    //基本用不到
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        //只有回话窗口才有
                        System.out.println("merge方法");
                        return 0;
                    }
                }
        );

        aggregate.print();

        env.execute();
    }
}
