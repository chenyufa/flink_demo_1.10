package util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ date: 2020/03/31 17:14
 * @ author: Cheney
 * @ description: 单词计算
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word: sentence.split(" ")) {
            out.collect(new Tuple2<String, Integer>(word, 1));
        }
    }

}
