package com.lfl.partition;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO DataStream实现Wordcount：读socket（无界流）
 *
 * @author cjp
 * @version 1.0
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("8.149.132.232", 7777);

        // shuffle 进行随机分区
//        socketDS.shuffle().print();


        // rebalance 进行轮询分区
        /*
        1> hello
        0> hello
        1> hello
        0> hello
        1> hello
         */
        socketDS.rebalance().print();
        /*
        缩放轮训分区
        局部组队比rebalance更加均匀和高效

         */
        socketDS.rescale().print();
        /*
        广播分区
        发送到所有分区
        全部都并行度收到
         */
        socketDS.broadcast();
        /*
        全局分区
        强行将并行度设置为1
        发送到所有分区
         */
        socketDS.global();







        env.execute();
    }
}

