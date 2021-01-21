package org.example.spark.pipelines;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.example.commons.GDELTHelper;
import scala.Tuple2;

public class SparkGDELTHelper {
  public static JavaPairRDD<String, String> extractCountrySubjectKVPairs(JavaRDD<String> input){
    return input.mapToPair((PairFunction<String, String, String>) inputString -> Tuple2
      .apply(GDELTHelper.getCountry(inputString), GDELTHelper.getSubject(inputString)));
  }
}
