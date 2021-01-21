package org.example.spark.pipelines;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

public class SimpleCombinePerKeyTest {
  private static String INPUT_LINES = "19790101\tAFR\tFRA\t043\t1\t4\t1\t2.8\t\t\t\n"
    + "19790101\tAFR\tFRA\t050\t2\t9\t1\t3.5\t\t\t\t\t\t\t\t\t\n"
    + "19790101\tAFX\tFRAGOV\t043\t2\t19\t1\t2.8\t1\t46\t2\t1\t46\t2\t1\t46\t2\n";

  @Test
  public void testSimpleCombinePerKey(){
    final String[] lines = INPUT_LINES.split("\n");
    SparkConf conf = new SparkConf();
    conf.setMaster("local[4]");
    conf.setAppName("test");
    final JavaSparkContext sparkContext = new JavaSparkContext(conf);
    final JavaRDD<String> inputRDD = sparkContext.parallelize(Arrays.asList(lines));
    Operator combine = new SimpleCombinePerKey();
    final List<Tuple2<String, Long>> collect = combine.apply(inputRDD).collect();
    assertThat(collect, hasItems(Tuple2.apply("AFR", 2L), Tuple2.apply("AFX", 1L)));
  }

}