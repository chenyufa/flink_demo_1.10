package function;

import entity.ProductViewData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @ date: 2020/04/24 15:54
 * @ author: Cheney
 * @ description: 自定义的聚合函数
 */
public class MyCountAggregate implements AggregateFunction<ProductViewData, Long, Long> {

    @Override
    public Long createAccumulator() {
        /*访问量初始化为0*/
        return 0L;
    }

    @Override
    public Long add(ProductViewData productViewData, Long aLong) {
        /*访问量直接+1 即可*/
        return aLong+1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }

}
