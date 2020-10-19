package function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ date: 2020/10/9 16:33
 * @ author: FatCheney
 * @ description:基于AggregateFunction 实现一个平均值的聚合函数
 * AggregateFunction 是 Flink 提供的一个通用的聚合函数实现，用户定义的聚合函数可以通过扩展 AggregateFunction 类来实现。
 * AggregateFunction 更加通用，它有 3 个参数：输入类型（IN）、累加器类型（ACC）和输出类型（OUT）。
 *
 * @ version: 1.0.0
 */
public class MyAverageAggregateFunction implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {

    // 用来创建一个累加器，负责将输入的数据进行迭代
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    // 该函数是用来将输入的每条数据和累加器进行计算的具体实现
    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
    }

    // 从累加器中获取计算结果
    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    // 将两个累加器进行合并
    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }

}
