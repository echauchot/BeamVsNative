package org.example.spark.pipelines;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class IdentityMap implements Operator<String> {

  @Override
  public JavaRDD<String> apply(JavaRDD<String> inputRDD) {
    return inputRDD.map((Function<String, String>) s -> s);
  }
}
