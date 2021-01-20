package org.example.beam.pipelines;

import static org.example.beam.pipelines.BeamGDELTHelper.extractCountrySubjectKVPairs;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class SimpleCombinePerKey extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

  @Override public PCollection<KV<String, Long>> expand(PCollection<String> input) {
    final PCollection<KV<String, String>> kvPCollection = extractCountrySubjectKVPairs(input);
      return kvPCollection.apply(Count.perKey());
  }

}
