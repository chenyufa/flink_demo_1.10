package operator.datadistinct;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ date: 2020/10/7 20:02
 * @ author: FatCheney
 * @ description: 基于布隆过滤器（BloomFilter） 的去重
 * BloomFilter（布隆过滤器）类似于一个 HashSet，用于快速判断某个元素是否存在于集合中，
 * 其典型的应用场景就是能够快速判断一个 key 是否存在于某容器，不存在就直接返回。
 * 和 HyperLogLog 一样，布隆过滤器不能保证 100% 精确。但是它的插入和查询效率都很高。
 * @ version: 1.0.0
 */
public class BloomFilterDistinct extends KeyedProcessFunction<Long, String, Long> {

    private transient ValueState<BloomFilter> bloomState;

    private transient ValueState<Long> countState;

    /**
     * 使用 Guava 自带的 BloomFilter，每当来一条数据时，就检查 state 中的布隆过滤器中是否存在当前的 SKU，如果没有则初始化，如果有则数量加 1。
     * @param value 一条条数据
     * @param context 上下文
     * @param collector 收集返回器
     * @throws Exception
     */
    @Override
    public void processElement(String value, Context context, Collector<Long> collector) throws Exception {

        BloomFilter bloomFilter = bloomState.value();
        Long skuCount = countState.value();
        if(bloomFilter == null){
            BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
        }
        if(skuCount == null){
            skuCount = 0L;
        }
        if(!bloomFilter.mightContain(value)){
            bloomFilter.put(value);
            skuCount = skuCount + 1;
        }
        bloomState.update(bloomFilter);
        countState.update(skuCount);
        collector.collect(countState.value());

    }

}
