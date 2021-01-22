package org.example.commons;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;

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
    String result = String.format("%s;%s;%s;%s;%s;%s", benchType, benchEngine, benchPipeline, inputFile, runTime);

    Calendar calendar = Calendar.getInstance();
    String outputFilePath = String
      .format("%s/%s.%s.%s.%s%s%s%s%s%s", outputDir, benchType, benchEngine, benchPipeline,
        calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
        calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR),
        calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
    FileOutputStream fos = new FileOutputStream(outputFilePath);
    fos.write(header.getBytes(StandardCharsets.UTF_8));
    fos.write(result.getBytes(StandardCharsets.UTF_8));
    fos.close();
  }
}
