package com.lfl.watemark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author 叶星痕
 * @data 2024/5/24 下午5:41
 * 文件名 : MyPeriodWatermarkGenerator
 * 描述 :
 */
public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    private long delayTS;
    //用来保存当前位置最大的事件时间
    private long maxTS;


    public MyPeriodWatermarkGenerator(long delayTS) {

        this.delayTS = delayTS;
        this.maxTS = Long.MIN_VALUE + this.delayTS + 1;
    }

    /**
     * 为每个事件调用，允许水印生成器检查并记住事件
     * 时间戳，或者基于事件本身发出水印。
     *
     * @param event
     * @param eventTimestamp
     * @param output         MyPeriodWatermarkGenerator
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTS = Math.max(maxTS, eventTimestamp);
    }

    /**
     * 定期调用，可能会发出新的水印，也可能不会。
     * <p>调用此方法和生成水印的间隔取决于
     * {
     * 链接
     * ExecutionConfig#getAutoWatermarkInterval（）｝。
     * @param输出
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        //生成watemark
        output.emitWatermark(new Watermark(maxTS - delayTS - 1));
    }
}
