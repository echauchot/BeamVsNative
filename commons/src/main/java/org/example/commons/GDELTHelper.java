package org.example.commons;

public class GDELTHelper {
  public static String getCountry(String row) {
    String[] fields = row.split("\\t+");
    return fields[1];
  }

  public static String getSubject(String row) {
    String[] fields = row.split("\\t+");
    return fields[3];
  }

}
