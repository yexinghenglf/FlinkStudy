package com.lfl.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 叶星痕
 * @data 2024/4/6 下午2:55
 * 文件名 : CollectionDemo
 * 描述 :
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合读取数据
        DataStreamSource<Integer> source =
                env.fromElements(1, 22, 333);

        source.print();

        env.execute();
    }
}
