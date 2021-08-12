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
package org.apache.geode.internal.util.redaction;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.util.redaction.RedactionDefaults.REDACTED;
import static org.apache.geode.internal.util.redaction.RedactionDefaults.SENSITIVE_PREFIXES;
import static org.apache.geode.internal.util.redaction.RedactionDefaults.SENSITIVE_SUBSTRINGS;

import java.util.Collection;
import java.util.List;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Redacts value strings for keys that are identified as sensitive data.
 */
public class StringRedaction implements SensitiveDataDictionary {

  private final String redacted;
  private final SensitiveDataDictionary sensitiveDataDictionary;
  private final RedactionStrategy redactionStrategy;

  public StringRedaction() {
    this(REDACTED,
        new CombinedSensitiveDictionary(
            new SensitivePrefixDictionary(SENSITIVE_PREFIXES),
            new SensitiveSubstringDictionary(SENSITIVE_SUBSTRINGS)));
  }

  private StringRedaction(String redacted, SensitiveDataDictionary sensitiveDataDictionary) {
    this(redacted,
        sensitiveDataDictionary,
        new RegexRedactionStrategy(sensitiveDataDictionary::isSensitive, redacted));
  }

  @VisibleForTesting
  StringRedaction(String redacted, SensitiveDataDictionary sensitiveDataDictionary,
      RedactionStrategy redactionStrategy) {
    this.redacted = redacted;
    this.sensitiveDataDictionary = sensitiveDataDictionary;
    this.redactionStrategy = redactionStrategy;
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
  public String redact(String string) {
    return redactionStrategy.redact(string);
  }

  public String redact(Iterable<String> strings) {
    return redact(String.join(" ", strings));
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
  public String redactArgumentIfNecessary(String key, String value) {
    if (isSensitive(key)) {
      return redacted;
    }
    return value;
  }

  public List<String> redactEachInList(Collection<String> strings) {
    return strings.stream()
        .map(this::redact)
        .collect(toList());
  }

  @Override
  public boolean isSensitive(String key) {
    return sensitiveDataDictionary.isSensitive(key);
  }

  public String getRedacted() {
    return redacted;
  }
}
