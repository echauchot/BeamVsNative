package org.example.spark.pipelines;

import static org.example.spark.pipelines.SparkGDELTHelper.extractCountrySubjectKVPairs;

import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public class SimpleCombinePerKey implements Operator<String> {

  @Override public AbstractJavaRDDLike<?, ?> apply(JavaRDD<String> inputRDD) {
    final JavaPairRDD<String, String> kvPairRDD = extractCountrySubjectKVPairs(
      inputRDD);
    return kvPairRDD.aggregateByKey(0L, (Function2<Long, String, Long>) (l, s) -> l++,
      (Function2<Long, Long, Long>) (l1, l2) -> l1 + l2);
  }
}
