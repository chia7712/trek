package com.spright.trek.utils;

import com.sun.net.httpserver.Headers;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpUtils {

  private HttpUtils() {
  }

  public static Optional<Long> parseForContentLength(Map<String, List<String>> fieldHeader) {
    for (Map.Entry<String, List<String>> entry : fieldHeader.entrySet()) {
      if (entry.getKey().equalsIgnoreCase("Content-length")) {
        for (String s : entry.getValue()) {
          try {
            return Optional.of(Long.valueOf(s.replaceAll(" ", "")));
          } catch (NumberFormatException e) {
          }
        }
      }
    }
    return Optional.empty();
  }

  public static Optional<String> parseForBoundary(Headers fieldHeader) {
    for (Map.Entry<String, List<String>> entry : fieldHeader.entrySet()) {
      if (entry.getKey().equalsIgnoreCase("Content-type")) {
        for (String s : entry.getValue()) {
          if (s.contains("boundary")) {
            int index = s.indexOf("=");
            if (index != -1) {
              return Optional.of(s.substring(index + 1));
            }
          }
        }
      }
    }
    return Optional.empty();
  }

  public static Optional<String> parseForFileName(Map<String, List<String>> fieldHeader) {
    for (Map.Entry<String, List<String>> entry : fieldHeader.entrySet()) {
      if (entry.getKey().equalsIgnoreCase("content-disposition")) {
        for (String s : entry.getValue()) {
          final int index = s.indexOf("filename");
          if (index == -1) {
            continue;
          }
          String nameString = s.substring(index + "filename".length());
          if (nameString.length() > 2
                  && nameString.startsWith("\"") && nameString.endsWith("\"")) {
            nameString = nameString.substring(1, nameString.length() - 1);
          }
          return Optional.of(nameString);
        }
      }
    }
    return Optional.empty();
  }
}
