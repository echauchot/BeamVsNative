package org.example.beam.pipelines;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import static org.example.beam.pipelines.BeamGDELTHelper.extractCountrySubjectKVPairs;

public class SimpleGroupByKey extends
  PTransform<PCollection<String>, PCollection<KV<String, Iterable<String>>>> {

  @Override
  public PCollection<KV<String, Iterable<String>>> expand(PCollection<String> input) {
    final PCollection<KV<String, String>> kvPCollection = extractCountrySubjectKVPairs(input);
    return kvPCollection.apply(GroupByKey.create());
  }
}
