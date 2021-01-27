package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.example.commons.BenchmarkHelper;

/**
 * CombinePerKey that counts the occurences per key. It is lowest level.
 */
public class SimpleCombinePerKey implements Operator<String>{

  @Override
  public DataSet<Tuple2<String, Long>> apply(DataSet<String> inputDataSet) {
    final MapOperator<String, Tuple2<String, Long>> kvDataSet = inputDataSet.map(new MapFunction<String, Tuple2<String, Long>>() {
      // use anonymous class instead of lambda to provide type information
      @Override public Tuple2<String, Long> map(String s) throws Exception {
        return Tuple2.of(BenchmarkHelper.getCountry(s), 1L);
      }
    });
    // groupby is needed for flink but happens behind the scenes for spark/beam
    return kvDataSet.groupBy(new KeySelector<Tuple2<String, Long>, String>() {
      @Override public String getKey(Tuple2<String, Long> tuple2) throws Exception {
        return tuple2.f0;
      }
    })
      // use anonymous class instead of lambda to provide type information
      .reduce(new ReduceFunction<Tuple2<String, Long>>() {
        @Override public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
          return Tuple2.of(t1.f0, t1.f1 + t2.f1);
        }
      });
  }
}
