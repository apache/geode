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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ArgumentRedactor {

  private ArgumentRedactor() {}

  public static String redact(final List<String> args) {
    StringBuilder redacted = new StringBuilder();
    for (String arg : args) {
      redacted.append(redact(arg)).append(" ");
    }
    return redacted.toString().trim();
  }

  /**
   * Accept a map of key/value pairs and produce a printable string, redacting any necessary values.
   * 
   * @param map A {@link Map} of key/value pairs such as a collection of properties
   *
   * @return A printable string with redacted fields. E.g., "username=jdoe password=********"
   */
  public static String redact(final Map<String, String> map) {
    StringBuilder redacted = new StringBuilder();
    for (Entry<String, String> entry : map.entrySet()) {
      redacted.append(entry.getKey());
      redacted.append("=");
      redacted.append(redact(entry));
      redacted.append(" ");
    }
    return redacted.toString().trim();
  }

  /**
   * Returns the redacted value of the {@link Entry} if the key indicates redaction is necessary.
   * Otherwise, value is returned, unchanged.
   * 
   * @param entry A key/value pair
   *
   * @return The redacted string for value.
   */
  public static String redact(Entry<String, String> entry) {
    return redact(entry.getKey(), entry.getValue());
  }

  /**
   * Parse a string to find key=value pairs and redact the values if necessary. If more than one
   * key=value pair exists in the input, each pair must be preceeded by a hyphen '-' to delineate
   * the pairs. <br>
   * Example:<br>
   * Single value: "password=secret" or "--password=secret" Mulitple values: "-Dflag -Dkey=value
   * --classpath=."
   * 
   * @param line The input to be parsed
   * @return A redacted string that has sensitive information obscured.
   */
  public static String redact(String line) {
    StringBuilder redacted = new StringBuilder();
    if (line.startsWith("-")) {
      line = " " + line;
      String[] args = line.split(" -");
      StringBuilder param = new StringBuilder();
      for (String arg : args) {
        if (arg.isEmpty()) {
          param.append("-");
        } else {
          String[] pair = arg.split("=", 2);
          param.append(pair[0].trim());
          if (pair.length == 1) {
            redacted.append(param);
          } else {
            redacted.append(param).append("=").append(redact(param.toString(), pair[1].trim()));
          }
          redacted.append(" ");
        }
        param.setLength(0);
        param.append("-");
      }
    } else {
      String[] args = line.split("=", 2);
      if (args.length == 1) {
        redacted.append(line);
      } else {
        redacted.append(args[0].trim()).append("=").append(redact(args[0], args[1]));
      }
      redacted.append(" ");
    }
    return redacted.toString().trim();
  }

  /**
   * Return a redacted value if the key indicates redaction is necessary. Otherwise, return the
   * value unchanged.
   *
   * @param key A string such as a system property, jvm parameter or similar in a key=value
   *        situation.
   * @param value A string that is the value assigned to the key.
   *
   * @return A redacted string if the key indicates it should be redacted, otherwise the string is
   *         unchanged.
   */
  public static String redact(String key, String value) {
    if (shouldBeRedacted(key)) {
      return "********";
    }
    return value.trim();
  }

  /**
   * Determine whether a key's value should be redacted.
   * 
   * @param key The key in question.
   *
   * @return true if the value should be redacted, otherwise false.
   */
  private static boolean shouldBeRedacted(String key) {
    if (key == null) {
      return false;
    }

    // Clean off any flags such as -J and -D to get to the actual start of the parameter
    String compareKey = key;
    if (key.startsWith("-J")) {
      compareKey = key.substring(2);
    }
    if (compareKey.startsWith("-D")) {
      compareKey = compareKey.substring(2);
    }
    return compareKey.toLowerCase().contains("password");
    // return compareKey
    // .startsWith(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.SECURITY_PREFIX_NAME)
    // || compareKey.startsWith(
    // DistributionConfigImpl.SECURITY_SYSTEM_PREFIX + DistributionConfig.SECURITY_PREFIX_NAME)
    // || compareKey.toLowerCase().contains("password");
  }
}
