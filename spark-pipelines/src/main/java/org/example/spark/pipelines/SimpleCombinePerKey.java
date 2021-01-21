package org.example.spark.pipelines;

import static org.example.spark.pipelines.SparkGDELTHelper.extractCountrySubjectKVPairs;

import java.util.Map;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SimpleCombinePerKey implements Operator<String> {

  @Override public AbstractJavaRDDLike<?, ?> apply(JavaRDD<String> inputRDD) {
    final JavaPairRDD<String, String> kvPairRDD = extractCountrySubjectKVPairs(
      inputRDD);
    return kvPairRDD.aggregateByKey(0, (Function2<Integer, String, Integer>) (integer, s) -> integer++,
      (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
  }
}
