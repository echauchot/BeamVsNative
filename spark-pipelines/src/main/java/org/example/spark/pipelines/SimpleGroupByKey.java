package org.example.spark.pipelines;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
    return kvPairRDD.groupByKey();
  }
}
