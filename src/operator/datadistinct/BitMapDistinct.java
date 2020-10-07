package operator.datadistinct;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * @ date: 2020/10/7 20:07
 * @ author: FatCheney
 * @ description: 基于 BitMap 的去重
 * HyperLogLog 和 BloomFilter 虽然减少了存储但是丢失了精度， 这在某些业务场景下是无法被接受的。下面的这种方法不仅可以减少存储，而且还可以做到完全准确，那就是使用 BitMap
 * Bit-Map 的基本思想是用一个 bit 位来标记某个元素对应的 Value，而 Key 即是该元素。由于采用了 Bit 为单位来存储数据，因此可以大大节省存储空间。
 * 需要添加maven依赖：org.roaringbitmap，RoaringBitmap，0.8.13
 * @ version: 1.0.0
 */
public class BitMapDistinct implements AggregateFunction<Long, Roaring64NavigableMap,Long> {

    @Override
    public Roaring64NavigableMap createAccumulator() {
        return new Roaring64NavigableMap();
    }

    /**
     * Roaring64NavigableMap，其是 BitMap 的一种实现
     * 我们的数据是每次被访问的 SKU，把它直接添加到 Roaring64NavigableMap
     * @param value 数据
     * @param accumulator Roaring64NavigableMap
     * @return
     */
    @Override
    public Roaring64NavigableMap add(Long value, Roaring64NavigableMap accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Long getResult(Roaring64NavigableMap accumulator) {
        // 直接获取结果
        return accumulator.getLongCardinality();
    }

    @Override
    public Roaring64NavigableMap merge(Roaring64NavigableMap a, Roaring64NavigableMap b) {
        return null;
    }

}
