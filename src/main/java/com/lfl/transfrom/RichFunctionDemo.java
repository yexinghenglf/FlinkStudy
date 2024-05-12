package com.lfl.transfrom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/4/7 下午5:54
 * 文件名 : RichFunction
 * 描述 :
 */
public class RichFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("此子任务的索引 = " + indexOfThisSubtask + " 带子任务的任务名称 = "
                        + taskNameWithSubtasks);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public Integer map(Integer value) throws Exception {

                return value + 1;
            }
        });

        map.print();

    }
}
