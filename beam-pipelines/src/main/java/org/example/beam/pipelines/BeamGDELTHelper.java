package org.example.beam.pipelines;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import static org.example.commons.BenchmarkHelper.getCountry;
import static org.example.commons.BenchmarkHelper.getSubject;

public class BeamGDELTHelper {
  public static PCollection<KV<String, String>> extractCountrySubjectKVPairs(PCollection<String> input){
    return input
      .apply("ExtractCountrySubjectKVPairs", ParDo.of(new DoFn<String, KV<String, String>>() {
        @ProcessElement
        public void processElements(ProcessContext context){
          final String element = context.element();
          context.output(KV.of(getCountry(element), getSubject(element)));
        }
      }));
  }

}
