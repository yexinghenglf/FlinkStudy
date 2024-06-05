package com.lfl.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 叶星痕
 * @data 2024/6/5 下午3:20
 * 文件名 : OperatorListState
 * 描述 :
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("idea", 7777)
                .map(new MyCountMapFunction())
                .print();

        env.execute();
    }


    // TODO 实现接口
    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {


        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            return count++;

        }

        /**
         * 本地变量持久化
         *
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState.....................................");

            //清空算子内容
            state.clear();

            //本地变量加入到算子转态中
            state.add(count);

        }

        /**
         * 初始化本地变量
         *
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            //获取算子状态
            ListState<Long> state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("state", Types.LONG)
            );

            //把数据放到本地变量
            if (context.isRestored()) {
                for (Long c : state.get()) {
                    count += c;
                }
            }

        }
    }
}
