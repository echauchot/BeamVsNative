package org.example.flink.pipelines;

import org.apache.flink.api.java.DataSet;

public interface Operator<T> {
  void apply(DataSet<T> inputDataSet);
}
