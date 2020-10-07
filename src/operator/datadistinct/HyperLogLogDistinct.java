package operator.datadistinct;

import net.agkn.hll.HLL;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ date: 2020/10/7 19:57
 * @ author: FatCheney
 * @ description: 基于 HyperLogLog 的去重
 * 注意：HyperLogLog 并不是精准的去重
 * HyperLogLog 是一种估计统计算法，被用来统计一个集合中不同数据的个数，也就是我们所说的去重统计。
 * HyperLogLog 算法是用于基数统计的算法，每个 HyperLogLog 键只需要花费 12 KB 内存，就可以计算接近 2 的 64 方个不同元素的基数。
 * HyperLogLog 适用于大数据量的统计，因为成本相对来说是更低的，最多也就占用 12KB 内存。
 * 需要添加maven依赖：net.agkn，hll，1.6.0
 * @ version: 1.0.0
 */
public class HyperLogLogDistinct implements AggregateFunction<Tuple2<String,Long>, HLL,Long> {

    @Override
    public HLL createAccumulator() {
        return new HLL(14, 5);
    }

    @Override
    public HLL add(Tuple2<String, Long> value, HLL accumulator) {
        // addRaw 方法用于向 HyperLogLog 中插入元素。如果插入的元素非数值型的，则需要 hash 过后才能插入。
        //value 为访问记录 <商品sku, 用户id>
        accumulator.addRaw(value.f1);
        return accumulator;
    }

    @Override
    public Long getResult(HLL accumulator) {
        // accumulator.cardinality() 方法用于计算 HyperLogLog 中元素的基数
        long cardinality = accumulator.cardinality();
        return cardinality;
    }

    @Override
    public HLL merge(HLL a, HLL b) {
        a.union(b);
        return a;
    }

}
