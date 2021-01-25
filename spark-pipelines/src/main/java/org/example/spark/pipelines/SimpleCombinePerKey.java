package org.example.spark.pipelines;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.example.commons.BenchmarkHelper;
import scala.Tuple2;

/**
 * CombinePerKey that counts the occurences per key. It is lowest level (do not use count operator).
 */
public class SimpleCombinePerKey implements Operator<String> {

  @Override
  public JavaPairRDD<String, Long> apply(JavaRDD<String> inputRDD) {
    final JavaPairRDD<String, Long> kvPairRDD = inputRDD.mapToPair(
      (PairFunction<String, String, Long>) s -> Tuple2.apply(BenchmarkHelper.getCountry(s), 1L));
    return kvPairRDD.aggregateByKey(0L, (Function2<Long, Long, Long>) (l, s) -> ++l,
      (Function2<Long, Long, Long>) (l1, l2) -> l1 + l2);
  }
}
