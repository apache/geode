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

import java.util.Collection;
import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.util.redaction.StringRedaction;

public class ArgumentRedactor {

  @Immutable
  private static final StringRedaction DELEGATE = new StringRedaction();

  private ArgumentRedactor() {
    // do not instantiate
  }

  /**
   * Parse a string to find key/value pairs and redact the values if identified as sensitive.
   *
   * <p>
   * The following format is expected:<br>
   * - Each key/value pair should be separated by spaces.<br>
   * - The key must be preceded by '--', '-D', or '--J=-D'.<br>
   * - The value may optionally be wrapped in quotation marks.<br>
   * - The value is assigned to a key with '=', '=' padded with any number of optional spaces, or
   * any number of spaces without '='.<br>
   * - The value must not contain spaces without being wrapped in quotation marks.<br>
   * - The value may contain spaces or any other symbols when wrapped in quotation marks.
   *
   * <p>
   * Examples:
   * <ol>
   * <li>"--password=secret"
   * <li>"--user me --password secret"
   * <li>"-Dflag -Dopt=arg"
   * <li>"--classpath=."
   * <li>"password=secret"
   * </ol>
   *
   * @param string The string input to be parsed
   *
   * @return A string that has sensitive data redacted.
   */
  public static String redact(String string) {
    return DELEGATE.redact(string);
  }

  public static String redact(Iterable<String> strings) {
    return DELEGATE.redact(strings);
  }

  /**
   * Return the redacted value string if the provided key is identified as sensitive, otherwise
   * return the original value.
   *
   * @param key A string such as a system property, java option, or command-line key.
   * @param value The string value for the key.
   *
   * @return The redacted string if the key is identified as sensitive, otherwise the original
   *         value.
   */
  public static String redactArgumentIfNecessary(String key, String value) {
    return DELEGATE.redactArgumentIfNecessary(key, value);
  }

  public static List<String> redactEachInList(Collection<String> strings) {
    return DELEGATE.redactEachInList(strings);
  }

  /**
   * Returns true if a string identifies sensitive data. For example, a string containing
   * the word "password" identifies data that is sensitive and should be secured.
   *
   * @param key The string to be evaluated.
   *
   * @return true if the string identifies sensitive data.
   */
  public static boolean isSensitive(String key) {
    return DELEGATE.isSensitive(key);
  }

  public static String getRedacted() {
    return DELEGATE.getRedacted();
  }
}
