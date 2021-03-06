package org.example.flink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.example.commons.BenchmarkHelper;
import org.example.flink.pipelines.IdentityMap;
import org.example.flink.pipelines.Operator;
import org.example.flink.pipelines.SimpleCombinePerKey;
import org.example.flink.pipelines.SimpleGroupByKey;
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

    final String flinkMaster = parameters.get("--flinkMaster");
    if (Strings.isNullOrEmpty(flinkMaster)){
      throw new RuntimeException("Please specify a valid flinkMaster");
    }

    final String parallelism = parameters.get("--parallelism");
    if (Strings.isNullOrEmpty(parallelism)){
      throw new RuntimeException("Please specify a valid parallelism");
    }

    final Operator<String> operator = instanciateOperator(pipelineToRun);
    ExecutionEnvironment env;
    if ("[local]".equals(flinkMaster)){
      env = ExecutionEnvironment.createLocalEnvironment();
    } else { // use [auto] that works for cluster execution as well
      env = ExecutionEnvironment.getExecutionEnvironment();
    }
    env.setParallelism(Integer.parseInt(parallelism));
    DataSet<String> inputDataStream = env.readTextFile(inputFile);
    final DataSet<?> resultDataSet = operator.apply(inputDataStream);
    resultDataSet.output(new NoOpOutputFormat());

    LOG.info("Benchmark starting on Flink");
    final long start = System.currentTimeMillis();
    env.execute();
    final long end = System.currentTimeMillis();
    final long runtime = (end - start) / 1000;
    LOG.info("Pipeline {} ran in {} s on Flink", pipelineToRun, runtime);
    BenchmarkHelper.logResultsToFile("native", "flink", pipelineToRun, inputFile, runtime, outputDir);
  }
  private static class NoOpOutputFormat implements OutputFormat{
    @Override
    public void configure(Configuration configuration) {

    }

    @Override public void open(int i, int i1) throws IOException {

    }

    @Override public void writeRecord(Object o) throws IOException {

    }

    @Override public void close() throws IOException {

    }

  }
}
