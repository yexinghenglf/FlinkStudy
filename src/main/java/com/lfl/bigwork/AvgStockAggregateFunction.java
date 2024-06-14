package com.lfl.bigwork;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public  class AvgStockAggregateFunction implements AggregateFunction<Stock, Tuple2<Double, Integer>, Double> {
    @Override
    public Tuple2<Double, Integer> createAccumulator() {
        return Tuple2.of(0.0, 0);
    }

    @Override
    public Tuple2<Double, Integer> add(Stock value, Tuple2<Double, Integer> accumulator) {
        return Tuple2.of(accumulator.f0 + value.getPrice() * value.getVolume(), accumulator.f1 + value.getVolume());
    }

    @Override
    public Double getResult(Tuple2<Double, Integer> accumulator) {
        return accumulator.f0 / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}

