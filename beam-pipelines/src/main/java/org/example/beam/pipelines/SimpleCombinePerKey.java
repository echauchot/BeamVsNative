package org.example.beam.pipelines;

import static org.example.beam.pipelines.BeamGDELTHelper.extractCountrySubjectKVPairs;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SimpleCombinePerKey extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

  @Override public PCollection<KV<String, Long>> expand(PCollection<String> input) {
    final PCollection<KV<String, String>> kvPCollection = extractCountrySubjectKVPairs(input);
      return kvPCollection.apply(Combine.perKey(new Combine.CombineFn<String, Long, Long>() {

        @Override public Long createAccumulator() {
          return 0L;
        }

        @Override public Long addInput(Long mutableAccumulator, String input) {
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
