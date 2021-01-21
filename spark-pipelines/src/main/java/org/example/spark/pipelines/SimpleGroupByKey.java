package org.example.spark.pipelines;

import static org.example.spark.pipelines.SparkGDELTHelper.extractCountrySubjectKVPairs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class SimpleGroupByKey implements Operator<String> {

  @Override public JavaPairRDD<String, Iterable<String>> apply(JavaRDD<String> inputRDD) {
    final JavaPairRDD<String, String> kvPairRDD = extractCountrySubjectKVPairs(inputRDD);
    return kvPairRDD.groupByKey();
  }
}
