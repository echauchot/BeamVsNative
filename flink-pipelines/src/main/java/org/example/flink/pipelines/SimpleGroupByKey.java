package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.util.Collector;
import org.example.commons.BenchmarkHelper;
import scala.Tuple2;

/**
 * GroupByKey with shuffle (the whole record will be shuffled to group records with the same key)
 */

public class SimpleGroupByKey implements Operator<String>{

  @Override
  public DataSet<?> apply(DataSet<String> inputDataSet) {
    final DataSet<Tuple2<String, String>> kvDataSet = inputDataSet.map(
      (MapFunction<String, Tuple2<String, String>>) s -> Tuple2.apply(BenchmarkHelper.getCountry(s), s));
    final UnsortedGrouping<Tuple2<String, String>> unsortedGrouping = kvDataSet
      .groupBy((KeySelector<Tuple2<String, String>, String>) tuple2 -> tuple2._1());
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
