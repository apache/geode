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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArgumentRedactor {
  private static final String redacted = "********";

  // All taboo words should be entirely lowercase.
  private static final List<String> tabooWords = ArrayUtils.asList("password");

  // This pattern consists of three capture groups:
  // The option, consisting of
  // (a) one or two hyphens
  // (b) one or more non-whitespace, non-"=" characters, matching greedily
  // The option-value separator, consisting of: any amount of whitespace surrounding at most 1 "="
  // The value, consisting of:
  // (a) If not wrapped in quotes, all non-whitespace characters, matching greedily.
  // (b) If wrapped in quotes, any non-quote character, matching greedily, until the closing quote.
  // -- -- This will therefore break on, e.g., --opt="escaped \" quote" and only redact "escaped."
  // Positive lookahead between groups 1 and 2 to require space or "=", while * and ? match empty.
  // Negative lookahead between groups 2 and 3 to avoid "--boolFlag --newOption" matching as a pair.
  private static final Pattern optionWithValuePattern =
      Pattern.compile("(--?[^\\s=]+)(?=[ =])( *=? *)(?!-)((?:\"[^\"]*\"|\\S+))");

  private ArgumentRedactor() {}

  /**
   * Parse a string to find key-value pairs and redact the values if necessary.<br>
   *
   * The following format is expected:<br>
   * - Each key-value pair should be separated by spaces.<br>
   * - The key of each key-value pair must be preceded by a hyphen '-'.<br>
   * - Values may or may not be wrapped in quotation marks.<br>
   * - If a value is wrapped in quotation marks, the actual value should not contain any quotation
   * mark.<br>
   * - Keys and values may be separated by an equals sign '=' or any number of spaces.<br>
   * <br>
   * Examples:<br>
   * "--password=secret"<br>
   * "--user me --password secret"<br>
   * "-Dflag -Dkey=value"<br>
   * "--classpath=."<br>
   *
   * @param line The argument input to be parsed
   * @param permitFirstPairWithoutHyphen When true, prepends the line with a "-", which is later
   *        removed. This allows the use on, e.g., "password=secret" rather than "--password=secret"
   *
   * @return A redacted string that has sensitive information obscured.
   */
  public static String redact(String line, boolean permitFirstPairWithoutHyphen) {

    boolean wasPaddedWithHyphen = false;
    if (!line.trim().startsWith("-") && permitFirstPairWithoutHyphen) {
      line = "-" + line.trim();
      wasPaddedWithHyphen = true;
    }

    // We capture the key, separator, and values separately, replacing only the value at print.
    Matcher matcher = optionWithValuePattern.matcher(line);
    while (matcher.find()) {
      String option = matcher.group(1);
      if (!containsTabooWord(option)) {
        continue;
      }

      String separator = matcher.group(2);
      String withRedaction = option + separator + redacted;
      line = line.replace(matcher.group(), withRedaction);
    }

    if (wasPaddedWithHyphen) {
      line = line.substring(1);
    }
    return line;
  }

  /**
   * See {@link org.apache.geode.internal.util.ArgumentRedactor#redact(java.lang.String, boolean)}
   * This alias permits the first key-value pair to be given without a leading hyphen, so that
   * "password=secret" will be properly redacted.
   */
  public static String redact(String line) {
    return redact(line, true);
  }

  public static String redact(final List<String> args) {
    return redact(String.join(" ", args));
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
  public static String redactValueIfNecessary(String key, String value) {
    if (containsTabooWord(key)) {
      return redacted;
    }
    return value;
  }


  /**
   * Determine whether a key's value should be redacted.
   *
   * @param key The option key in question.
   *
   * @return true if the value should be redacted, otherwise false.
   */
  private static boolean containsTabooWord(String key) {
    if (key == null) {
      return false;
    }
    for (String taboo : tabooWords) {
      if (key.toLowerCase().contains(taboo)) {
        return true;
      }
    }
    return false;
  }
}
