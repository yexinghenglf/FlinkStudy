package com.lfl.split;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO DataStream实现Wordcount：读socket（无界流）
 *
 * @author cjp
 * @version 1.0
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("8.149.132.232", 7777);


        SingleOutputStreamOperator<String> oshu = socketDS.filter(value -> Integer.parseInt(value) % 2 == 0);
        SingleOutputStreamOperator<String> jishu = socketDS.filter(value -> Integer.parseInt(value) % 2 != 0);

        oshu.print("偶数");
        jishu.print("奇数");


        env.execute();
    }
}

