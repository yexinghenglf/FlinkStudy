package com.lfl.bigwork;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author 叶星痕
 * @data 2024/6/14 下午12:20
 * 文件名 : ResultWindowFunction
 * 描述 :
 */
public class ResultWindowFunction implements WindowFunction<Double, String, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Double> input, Collector<String> out)  {
        Double averagePrice = input.iterator().next();
        out.collect("股票代码：" + s + "，平均价格：" + averagePrice);
    }
}
