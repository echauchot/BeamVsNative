package org.example.flink.pipelines;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import scala.Tuple2;

public class SimpleGroupByKey implements Operator<String>{

  @Override
  public void apply(DataSet<String> inputDataSet) {
    final DataSet<Tuple2<String, String>> kvDataSet = FlinkGDELTHelper
      .extractCountrySubjectKVPairs(inputDataSet);
    kvDataSet.groupBy((KeySelector<Tuple2<String, String>, String>) tuple2 -> tuple2._1());
  }
}
