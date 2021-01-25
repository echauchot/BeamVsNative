package org.example.beam.pipelines;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.commons.BenchmarkHelper;

/**
 * GroupByKey with shuffle (the whole record will be shuffled to group records with the same key)
 */

public class SimpleGroupByKey extends
  PTransform<PCollection<String>, PCollection<KV<String, Iterable<String>>>> {

  @Override
  public PCollection<KV<String, Iterable<String>>> expand(PCollection<String> inputPCollection) {
    final PCollection<KV<String, String>> kvPCollection = inputPCollection.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
      @ProcessElement
      public void processElements(ProcessContext context){
        context.output(KV.of(BenchmarkHelper.getCountry(context.element()), context.element()));
      }
    }));
    return kvPCollection.apply(GroupByKey.create());
  }
}
