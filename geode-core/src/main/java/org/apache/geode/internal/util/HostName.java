/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.geode.internal.lang.SystemUtils;

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
          Scanner s = new Scanner(stream).useDelimiter(START_OF_STRING)) {
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
