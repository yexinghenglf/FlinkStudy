package com.lfl.bigwork;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class StockStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/stock.txt");

        DataStream<Stock> stockStream = source.map((MapFunction<String, Stock>) value -> {
            String[] fields = value.split(",");
            if (fields.length < 5) {
                throw new RuntimeException("Invalid input data: " + value);
            }
            return new Stock(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Integer.parseInt(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Stock>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> parseTimeToSeconds(event.getTradeTime())));

        KeyedStream<Stock, String> keyedStream = stockStream.keyBy((KeySelector<Stock, String>) Stock::getStockCode);

        SingleOutputStreamOperator<String> result = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(new AvgStockAggregateFunction(), new ResultWindowFunction());

        result.print();
//        result.writeAsText("output/avgStock.txt");


        env.execute();
    }

    //将形式为“HHmmss”的时间字符串解析为一天中的秒
    private static long parseTimeToSeconds(String time) {
        int hours = Integer.parseInt(time.substring(0, 2));
        int minutes = Integer.parseInt(time.substring(2, 4));
        int seconds = Integer.parseInt(time.substring(4, 6));
        return (hours * 3600L + minutes * 60L + seconds) * 1000L;
    }
}