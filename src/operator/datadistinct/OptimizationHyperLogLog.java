package operator.datadistinct;

import net.agkn.hll.HLL;
import redis.clients.util.MurmurHash;

import java.util.HashSet;
import java.util.Set;

/**
 * @ date: 2020/10/7 20:16
 * @ author: FatCheney
 * @ description: 优化
 * 在 HyperLogLog 去重实现中，如果要求误差在 0.001 以内，那么就需要 1048576 个 int, 也就是会消耗 4M 的存储空间，
 * 但是在实际使用中有很多的维度的统计是达不到这个数据量，那么可以在这里做一个优化，
 * 优化方式是：初始 HyperLogLog 内部使用存储是一个 set 集合，当 set 大小达到了指定大小(1048576)就转换为 HyperLogLog 存储方式。
 * 这种方式可以有效减小内存消耗。
 * @ version: 1.0.0
 */
public class OptimizationHyperLogLog {

    //hyperloglog 结构
    private HLL hyperLogLog;
    //初始的一个 set
    private Set<Integer> set;

    private double rsd;

    //hyperloglog 的桶个数，主要内存占用
    private int bucket;

    // 初始化：入参同样是一个允许的误差范围值 rsd，计算出 hyperloglog 需要桶的个数 bucket，也就需要是 int 数组大小，并且初始化一个 set 集合 hashset;
    public OptimizationHyperLogLog(double rsd){
        this.rsd=rsd;
        // TODO: 2020/10/7  this.bucket=1 << HLL.log2m(rsd);
        set=new HashSet<>();
    }

    // 数据插入：使用与 hyperloglog 同样的方式将插入数据转 hash,
    // 判断当前集合的大小+1 是否达到了 bucket，
    //   不满足则直接添加到 set 中，
    //   满足则将 set 里面数据转移到 hyperloglog 对象中并且清空 set, 后续数据将会被添加到 hyperloglog 中
    public void offer(Object object){
        // TODO: 2020/10/7  final int x = MurmurHash.hash(object);
        int currSize=set.size();
        if(hyperLogLog==null && currSize+1>bucket){
            //升级为 hyperloglog
            // TODO: 2020/10/7  hyperLogLog=new HLL(rsd);
            for(int d: set){
                //hyperLogLog.offerHashed(d);
            }
            set.clear();
        }

        if(hyperLogLog!=null){
            // TODO: 2020/10/7   hyperLogLog.offerHashed(x);
        }else {
            // TODO: 2020/10/7  set.add(x);
        }
    }

    //获取大小
    public long cardinality() {
        if(hyperLogLog!=null) return hyperLogLog.cardinality();
        return set.size();
    }

}
