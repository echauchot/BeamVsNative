package org.example.flink.pipelines;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.example.commons.BenchmarkHelper;
import scala.Tuple2;

public class FlinkGDELTHelper {

  public static DataSet<Tuple2<String, String>> extractCountrySubjectKVPairs(
    DataSet<String> input) {
    return input.map((MapFunction<String, Tuple2<String, String>>) s -> Tuple2
      .apply(BenchmarkHelper.getCountry(s), BenchmarkHelper.getSubject(s)));
  }

}
