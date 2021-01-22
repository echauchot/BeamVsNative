package org.example.beam.pipelines;


import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import static org.example.commons.BenchmarkHelper.getCountry;
import static org.example.commons.BenchmarkHelper.getSubject;

public class BeamGDELTHelper {
  public static PCollection<KV<String, String>> extractCountrySubjectKVPairs(PCollection<String> input){
    return input
      .apply("ExtractCountrySubjectKVPairs", MapElements
        .via(new SimpleFunction<String, KV<String, String>>() {

          @Override public KV<String, String> apply(String input) {
            return KV.of(getCountry(input), getSubject(input));
          }
        }));
  }

}
