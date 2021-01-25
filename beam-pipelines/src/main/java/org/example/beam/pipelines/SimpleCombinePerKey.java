package org.example.beam.pipelines;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.commons.BenchmarkHelper;

/**
 * CombinePerKey that counts the occurences per key. It is lowest level (do not use CountPerKey PTransform).
 * Also CombinePerKey is the lowest level combine for the runners (CombineGlobally keys the PCollection and applies CombinePerKey)
 */
public class SimpleCombinePerKey extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

  @Override public PCollection<KV<String, Long>> expand(PCollection<String> inputPCollection) {
    final PCollection<KV<String, Long>> kvPCollection = inputPCollection.apply(ParDo.of(new DoFn<String, KV<String, Long>>() {
      @ProcessElement
      public void processElement(ProcessContext context){
        context.output(KV.of(BenchmarkHelper.getCountry(context.element()), 1L));
      }
    }));
      return kvPCollection.apply(Combine.perKey(new Combine.CombineFn<Long, Long, Long>() {

        @Override public Long createAccumulator() {
          return 0L;
        }

        @Override public Long addInput(Long mutableAccumulator, Long input) {
          return ++mutableAccumulator;
        }

        @Override public Long mergeAccumulators(Iterable<Long> accumulators) {
          long result = 0;
          for (Long acc : accumulators){
            result += acc;
          }
          return result;
        }

        @Override public Long extractOutput(Long accumulator) {
          return accumulator;
        }
      }));
  }

}
