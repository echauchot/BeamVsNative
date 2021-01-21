package org.example.spark.pipelines;

import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;

public interface Operator<T> {
  AbstractJavaRDDLike<?, ?> apply(JavaRDD<T> inputRDD);
}
