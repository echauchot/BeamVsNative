package org.example.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BenchmarkHelperTest {

  private static String INPUT_LINES = "19790101\tAFR\tFRA\t043\t1\t4\t1\t2.8\t\t\t\n"
    + "19790101\tAFR\tFRA\t050\t2\t9\t1\t3.5\t\t\t\t\t\t\t\t\t\n"
    + "19790101\tAFR\tFRAGOV\t043\t2\t19\t1\t2.8\t1\t46\t2\t1\t46\t2\t1\t46\t2\n";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testExtractCountrySubjectKVPairs() {
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[0]));
    assertEquals("043", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[0]));
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[1]));
    assertEquals("050", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[1]));
    assertEquals("AFR", BenchmarkHelper.getCountry(INPUT_LINES.split("\n")[2]));
    assertEquals("043", BenchmarkHelper.getSubject(INPUT_LINES.split("\n")[2]));

  }

  @Test
  public void testLogResultsToFile() throws IOException {
    final String outputPath = temporaryFolder.getRoot().getPath();
    final String benchType = "beam";
    final String benchEngine = "spark";
    final String benchPipeline = "IdentityMap";
    BenchmarkHelper
      .logResultsToFile(benchType, benchEngine, benchPipeline, "inputFile", 10, outputPath);
    final String expectedFileNameFormat = String
      .format("%s.%s.%s", benchType, benchEngine, benchPipeline);
    final File outputDir = temporaryFolder.getRoot();
    final String[] outputFiles = outputDir.list((dir, name) -> name.contains(expectedFileNameFormat));
    assertTrue(outputFiles.length > 0);
  }
}