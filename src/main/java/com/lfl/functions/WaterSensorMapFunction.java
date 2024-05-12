package com.lfl.functions;

import com.lfl.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author 叶星痕
 * @data 2024/4/8 下午5:23
 * 文件名 : WaterSensorMapFunction
 * 描述 :
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }

}
