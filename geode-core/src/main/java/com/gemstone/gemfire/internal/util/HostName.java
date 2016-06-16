package com.gemstone.gemfire.internal.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.util.Scanner;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.sun.tools.doclets.internal.toolkit.util.DocFinder.Input;

public class HostName {

  static final String COMPUTER_NAME_PROPERTY = "COMPUTERNAME";
  static final String HOSTNAME_PROPERTY = "HOSTNAME";

  private static final String HOSTNAME = "hostname";
  private static final String START_OF_STRING = "\\A";
  private static final String UNKNOWN = "unknown";

  public String determineHostName() {
    String hostname = getHostNameFromEnv();
    if (isEmpty(hostname)) {
      hostname = execHostName();
    }
    assert !isEmpty(hostname);
    return hostname;
  }

  String execHostName() {
    String hostname;
    try {
      Process process = new ProcessBuilder(HOSTNAME).start();
      try (InputStream stream = process.getInputStream();
           Scanner s = new Scanner(stream).useDelimiter(START_OF_STRING);) {
        hostname = s.hasNext() ? s.next().trim() : UNKNOWN;
      }
    } catch (IOException hostnameBinaryNotFound) {
      hostname = UNKNOWN;
    }
    return hostname;
  }

  String getHostNameFromEnv() {
    final String hostname;
    if (SystemUtils.isWindows()) {
      hostname = System.getenv(COMPUTER_NAME_PROPERTY);
    } else {
      hostname = System.getenv(HOSTNAME_PROPERTY);
    }
    return hostname;
  }

  private boolean isEmpty(String value) {
    return value == null || value.isEmpty();
  }

}
