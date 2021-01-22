package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import scala.Tuple2;

public class SimpleCombinePerKey implements Operator{

  @Override public void apply(DataSet inputDataSet) {
    final DataSet<Tuple2<String, String>> kvDataSet = FlinkGDELTHelper.extractCountrySubjectKVPairs(inputDataSet);
    // groupby is need in flink but happens behind the scenes with spark and beam
    final UnsortedGrouping<Tuple2<String, String>> groupedDataSet = kvDataSet
      .groupBy((KeySelector<Tuple2<String, String>, String>) tuple2 -> tuple2._1());
    //TODO fix
    groupedDataSet.reduce((ReduceFunction<Tuple2<String, String>>) (tuple2, t1) -> null);

  }
}
