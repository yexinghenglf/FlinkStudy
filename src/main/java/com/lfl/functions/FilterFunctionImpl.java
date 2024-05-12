package com.lfl.functions;

import com.lfl.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author 叶星痕
 * @data 2024/4/7 下午5:50
 * 文件名 : FilterFunctionImpl
 * 描述 :
 */
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {
    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
