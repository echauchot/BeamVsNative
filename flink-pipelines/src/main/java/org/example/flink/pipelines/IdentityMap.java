package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

public class IdentityMap implements Operator<String> {

  @Override
  public void apply(DataSet<String> inputDataSet) {
    inputDataSet.map((MapFunction<String, String>) s -> s);
  }
}
