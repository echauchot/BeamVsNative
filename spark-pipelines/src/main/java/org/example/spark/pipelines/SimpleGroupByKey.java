package org.example.spark.pipelines;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.example.commons.BenchmarkHelper;
import scala.Tuple2;

/**
 * GroupByKey with shuffle (the whole record will be shuffled to group records with the same key)
 */

public class SimpleGroupByKey implements Operator<String> {

  @Override public JavaPairRDD<String, Iterable<String>> apply(JavaRDD<String> inputRDD) {
    final JavaPairRDD<String, String> kvPairRDD = inputRDD.mapToPair(
      (PairFunction<String, String, String>) s -> Tuple2.apply(BenchmarkHelper.getCountry(s), s));
    final JavaPairRDD<String, Iterable<String>> res = kvPairRDD.groupByKey();
    // not needed but as flink need to apply a reduce after groupby to get a dataset, apply
    // the simplest post processing after groupby to be comparable with flink
    return res.reduceByKey(
      (Function2<Iterable<String>, Iterable<String>, Iterable<String>>) (strings, strings2) -> Iterables
        .concat(strings, strings2));
  }
}
