package org.example.beam;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Strings;
import org.example.beam.pipelines.IdentityMap;
import org.example.beam.pipelines.SimpleCombinePerKey;
import org.example.beam.pipelines.SimpleGroupByKey;
import org.example.commons.BenchmarkHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Benchmark {

  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  private static PTransform<PCollection<String>, ? extends PCollection<? extends Serializable>> instanciatePTransform(
    String transformName) {
    switch (transformName) {
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

  public static void main(String[] args) throws Exception {

    final Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String pipelineToRun = pipelineOptions.getPipeline();
    if (Strings.isNullOrEmpty(pipelineToRun)){
      throw new RuntimeException("Please specify a valid pipeline among IdentityMap, SimpleGroupByKey or SimpleCombinePerKey");
    }
    final String inputFile = pipelineOptions.getInputFile();
    if (Strings.isNullOrEmpty(inputFile)){
      throw new RuntimeException("Please specify a valid GDELT REDUCED input file");
    }

    final String outputDir = pipelineOptions.getOutputDir();
    if (Strings.isNullOrEmpty(outputDir)){
      throw new RuntimeException("Please specify a valid results output directory");
    }

    Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<String> input = pipeline.apply("ReadFromGDELTFile", TextIO.read().from(inputFile));
      input.apply(pipelineToRun, instanciatePTransform(pipelineToRun));

    final String runnerName = pipelineOptions.getRunner().getSimpleName();
    LOG.info("Benchmark starting on Beam {} runner", runnerName);
    final long start = System.currentTimeMillis();
    pipeline.run().waitUntilFinish();
    final long end = System.currentTimeMillis();
    final long runtime = (end - start) / 1000;
    LOG.info("Pipeline {} ran in {} s on Beam {} runner", pipelineToRun, runtime, runnerName);
    BenchmarkHelper.logResultsToFile("beam", runnerName, pipelineToRun, inputFile, runtime, outputDir);
  }

  public interface Options extends PipelineOptions {

    @Description("Pipeline to run: IdentityMap, SimpleGroupByKey, SimpleCombinePerKey")
    String getPipeline();

    void setPipeline(String value);

    @Description("GDELT REDUCED input file to read")
    String getInputFile();

    void setInputFile(String value);

    @Description("results output directory")
    String getOutputDir();

    void setOutputDir(String value);
  }

}
