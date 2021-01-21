package org.example.spark;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

  private static final String INPUT_FILE = "/home/echauchot/projects/beamVsNative/input/GDELT.MASTERREDUCEDV2.TXT";
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  private static PTransform<PCollection<String>, ? extends PCollection<? extends Serializable>> instanciatePTransform(
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

    final Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String pipelineToRun = pipelineOptions.getPipeline();
    if (Strings.isNullOrEmpty(pipelineToRun)){
      throw new RuntimeException("Please specify a valid pipeline among IdentityPardo, SimpleGroupByKey or SimpleCombinePerKey");
    }
    Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<String> input = pipeline.apply("ReadFromGDELTFile", TextIO.read().from(INPUT_FILE));
    input.apply(pipelineToRun, instanciatePTransform(pipelineToRun));
    final String runnerName = pipelineOptions.getRunner().getSimpleName();
    LOG.info("Benchmark starting on Beam {} runner", runnerName);
    final long start = System.currentTimeMillis();
    pipeline.run();
    final long end = System.currentTimeMillis();
    LOG.info("Pipeline {} ran in {} s on Beam {} runner", pipelineToRun, (end - start) / 1000, runnerName);
  }

}
