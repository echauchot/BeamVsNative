package org.example.spark;

import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.commons.BenchmarkHelper;
import org.example.spark.pipelines.IdentityMap;
import org.example.spark.pipelines.Operator;
import org.example.spark.pipelines.SimpleCombinePerKey;
import org.example.spark.pipelines.SimpleGroupByKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  private static Operator<String> instanciateOperator(String operatorName) {
    switch (operatorName) {
      case "SimpleGroupByKey":
        return new SimpleGroupByKey();
      case "SimpleCombinePerKey":
        return new SimpleCombinePerKey();
      case "IdentityMap":
        return new IdentityMap();
      default:
        throw new RuntimeException("Please specify a valid pipeline among IdentityMap, SimpleGroupByKey or SimpleCombinePerKey");
    }
  }

  private static Map<String, String> extractParameters(String[] args){
    Map<String, String> result = new HashMap<>();
    for (String arg : args){
      final String key = arg.split("=")[0];
      final String value = arg.split("=")[1];
      result.put(key, value);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    final Map<String, String> parameters = extractParameters(args);

    final String pipelineToRun = parameters.get("--pipeline");
    if (Strings.isNullOrEmpty(pipelineToRun)){
      throw new RuntimeException("Please specify a valid pipeline among IdentityMap, SimpleGroupByKey or SimpleCombinePerKey");
    }
    final String inputFile = parameters.get("--inputFile");
    if (Strings.isNullOrEmpty(inputFile)){
      throw new RuntimeException("Please specify a valid GDELT REDUCED input file");
    }
    final String outputDir = parameters.get("--outputDir");
    if (Strings.isNullOrEmpty(outputDir)){
      throw new RuntimeException("Please specify a valid results output directory");
    }

    final String master = Strings.isNullOrEmpty(parameters.get("--master")) ? "local[4]" : parameters.get("--master");

    final Operator<String> operator = instanciateOperator(pipelineToRun);
    SparkConf conf = new SparkConf();
    conf.setMaster(master);
    conf.setAppName("BeamVsNative");
    final JavaSparkContext sparkContext = new JavaSparkContext(conf);
    final JavaRDD<String> inputRdd = sparkContext.textFile(inputFile);
    final AbstractJavaRDDLike<?, ?> resultRdd = operator.apply(inputRdd);
    LOG.info("Benchmark starting on Spark");
    final long start = System.currentTimeMillis();
    resultRdd.foreach(ignored ->{});
    final long end = System.currentTimeMillis();
    final long runtime = (end - start) / 1000;
    LOG.info("Pipeline {} ran in {} s on Spark", pipelineToRun, runtime);
    BenchmarkHelper.logResultsToFile("native", "spark", pipelineToRun, inputFile, runtime, outputDir);
  }

}
