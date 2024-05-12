package com.lfl.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/4/6 下午10:10
 * 文件名 : DataGeneratorDemo
 * 描述 :
 */
public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number: " + value;
                    }
                },
                10, //生成10条数据
                RateLimiterStrategy.perSecond(1)//一秒生成一条
                , Types.STRING
        );

        env.fromSource(dataGeneratorSource
                , WatermarkStrategy.noWatermarks()
                , "data-generator"
        )
                .print();


        env.execute();
    }
}
