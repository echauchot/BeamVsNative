package org.example.flink.pipelines;

import org.apache.flink.api.java.DataSet;

public interface Operator<T> {
  DataSet<?> apply(DataSet<T> inputDataSet);
}
