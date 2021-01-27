package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Lowest level identity map
 */
public class IdentityMap implements Operator<String> {

  @Override
  public DataSet<String> apply(DataSet<String> inputDataSet) {
    return inputDataSet.map(new MapFunction<String, String>() {
      @Override public String map(String element) throws Exception {
        return element;
      }
    });
  }
}
