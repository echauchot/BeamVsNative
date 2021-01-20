package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.example.pipelines.IdentityPardo;
import org.example.pipelines.SimpleCombinePerKey;
import org.example.pipelines.SimpleGroupByKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

  private static final String INPUT_FILE = "../input/GDELT.MASTERREDUCEDV2.TXT";
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  private static PTransform<PCollection<String>, PCollection<String>> instanciatePTransform(
    String transformName) {
    switch (transformName) {
      case "SimpleGroupByKey":
        return new SimpleGroupByKey();
      case "SimpleCombinePerKey":
        return new SimpleCombinePerKey();
      case "IdentityPardo":
        return new IdentityPardo();
      default:
        throw new RuntimeException("Please specify a valid pipeline among IdentityPardo, SimpleGroupByKey or SimpleCombinePerKey");
    }
  }

  public static void main(String[] args) throws Exception {

    final Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(Options.class);
    final String pipelineToRun = pipelineOptions.getPipeline();
    Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<String> input = pipeline.apply("ReadFromGDELTFile", TextIO.read().from(INPUT_FILE));
    input.apply(pipelineToRun, instanciatePTransform(pipelineToRun));
    final long start = System.currentTimeMillis();
    pipeline.run();
    final long end = System.currentTimeMillis();
    LOG.info("Pipeline {} ran in {} s on Beam {} runner", pipelineToRun, (end - start) / 1000, pipelineOptions.getRunner());
  }

  public interface Options extends PipelineOptions {

    @Description("Pipeline to run: IdentityPardo, SimpleGroupByKey, SimpleCombinePerKey") String getPipeline();

    void setPipeline(String value);
  }

}
