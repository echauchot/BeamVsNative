package org.example.commons;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BenchmarkHelper {

  /*
    GDELT helper methods
   */
  public static String getCountry(String row) {
    String[] fields = row.split("\\t+");
    return fields[1];
  }

  public static String getSubject(String row) {
    String[] fields = row.split("\\t+");
    return fields[3];
  }

  /*
    Benchmark input/output methods
   */
  public static void logResultsToFile(String benchType, String benchEngine, String benchPipeline, String inputFile, long runTime, String outputDir)
    throws IOException {
    String header = "benchType;benchEngine;benchPipeline;inputFile;runTime";
    String result = String.format("%s;%s;%s;%s;%s", benchType, benchEngine, benchPipeline, inputFile, runTime);

    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
    Date now = new Date();
    String outputFilePath = String
      .format("%s/%s.%s.%s.%s", outputDir, benchType, benchEngine, benchPipeline, sdfDate.format(now));
    new File(outputDir).mkdir();
    FileOutputStream fos = new FileOutputStream(outputFilePath);
    fos.write(header.getBytes(StandardCharsets.UTF_8));
    fos.write('\n');
    fos.write(result.getBytes(StandardCharsets.UTF_8));
    fos.close();
  }
}
