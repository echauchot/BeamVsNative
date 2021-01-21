package org.example.beam.pipelines;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class SimpleCombinePerKeyTest {

  private static String INPUT_LINES = "19790101\tAFR\tFRA\t043\t1\t4\t1\t2.8\t\t\t\n"
    + "19790101\tAFR\tFRA\t050\t2\t9\t1\t3.5\t\t\t\t\t\t\t\t\t\n"
    + "19790101\tAFX\tFRAGOV\t043\t2\t19\t1\t2.8\t1\t46\t2\t1\t46\t2\t1\t46\t2\n";

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testSimpleCombinePerKey(){
    final String[] lines = INPUT_LINES.split("\n");
    final PCollection<String> inputPCollection = testPipeline.apply(Create.of(Arrays.asList(lines)));
    final PCollection<KV<String, Long>> result = inputPCollection.apply(new SimpleCombinePerKey());
    PAssert.that(result).containsInAnyOrder(KV.of("AFR", 2L), KV.of("AFX", 1L));
    testPipeline.run();

  }
}