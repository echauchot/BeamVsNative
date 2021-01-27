package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.example.commons.BenchmarkHelper;

/**
 * GroupByKey with shuffle (the whole record will be shuffled to group records with the same key)
 */

public class SimpleGroupByKey implements Operator<String> {

  @Override public DataSet<Tuple2<String, String>> apply(DataSet<String> inputDataSet) {
    final DataSet<Tuple2<String, String>> kvDataSet = inputDataSet
      .map(new MapFunction<String, Tuple2<String, String>>() {
        @Override public Tuple2<String, String> map(String element) throws Exception {
          return Tuple2.of(BenchmarkHelper.getCountry(element), element);
        }
      });
    final UnsortedGrouping<Tuple2<String, String>> unsortedGrouping = kvDataSet
      .groupBy(new KeySelector<Tuple2<String, String>, String>() {
        @Override public String getKey(Tuple2<String, String> element) throws Exception {
          return element.f0;
        }
      });
    // in flink groupBy does not provide a DataSet a transformation needs to be applied to it to get the DataSet, so apply the simplest and more efficent one
    final GroupReduceOperator<Tuple2<String, String>, Tuple2<String, String>> resultDataSet = unsortedGrouping
      .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
        @Override public void reduce(Iterable<Tuple2<String, String>> iterable,
          Collector<Tuple2<String, String>> collector) throws Exception {
          iterable.forEach(tuple2 -> collector.collect(tuple2));
        }
      });
    return resultDataSet;
  }
}
