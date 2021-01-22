package org.example.commons;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BenchmarkHelperTest {

 private static String INPUT_LINES = "19790101\tAFR\tFRA\t043\t1\t4\t1\t2.8\t\t\t\n"
   + "19790101\tAFR\tFRA\t050\t2\t9\t1\t3.5\t\t\t\t\t\t\t\t\t\n"
   + "19790101\tAFR\tFRAGOV\t043\t2\t19\t1\t2.8\t1\t46\t2\t1\t46\t2\t1\t46\t2\n";
  @Test
  public void testExtractCountrySubjectKVPairs(){
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[0]));
    assertEquals("043", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[0]));
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[1]));
    assertEquals("050", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[1]));
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[2]));
    assertEquals("043", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[2]));

  }
}