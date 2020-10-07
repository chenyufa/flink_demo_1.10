package operator.datadistinct;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ date: 2020/10/7 19:47
 * @ author: FatCheney
 * @ description: 基于状态后端 的去重 ，利用 MapState 进行去重
 * @ version: 1.0.0
 */
public class MapStateDistinctFunction extends KeyedProcessFunction<String, Tuple2<String,Integer>,Tuple2<String,Integer>> {

    private transient ValueState<Integer> counts;

    @Override
    public void open(Configuration parameters) throws Exception {

        //自定义了状态的过期时间是 24 小时，在实际生产中大量的 Key 会使得状态膨胀，我们可以对存储的 Key 进行处理。例如，使用加密方法把 Key 加密成几个字节进再存储
        //我们设置 ValueState 的 TTL 的生命周期为24小时，到期自动清除状态
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.minutes(24 * 60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        //设置 ValueState 的默认值
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("skuNum", Integer.class);
        descriptor.enableTimeToLive(ttlConfig);
        counts = getRuntimeContext().getState(descriptor);
        super.open(parameters);

    }

    /**
     * 当一条数据经过，我们会在 MapState 中判断这条数据是否已经存在，如果不存在那么计数为 1，如果存在，那么在原来的计数上加 1
     * @param stringIntegerTuple2  Object
     * @param context Context
     * @param collector Collector
     * @throws Exception
     */
    @Override
    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {

        String f0 = stringIntegerTuple2.f0;
        //如果不存在则新增
        if(counts.value() == null){
            counts.update(1);
        }else{
            //如果存在则加1
            counts.update(counts.value()+1);
        }
        collector.collect(Tuple2.of(f0, counts.value()));

    }

}
