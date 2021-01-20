package org.example.commons;

public class GDELTHelper {
  public static String getCountry(String row) {
    String[] fields = row.split("\\t+");
    if (fields.length > 22) {
      if (fields[21].length() > 2) {
        return fields[21].substring(0, 1);
      }
      return fields[21];
    }
    return "NA";
  }

  public static String getSubject(String row) {
    String[] fields = row.split("\\t+");
    if (fields.length >= 7 && fields[6].length() > 0) {
      return fields[6];
    }
    return "NA";
  }

}
