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

import static java.util.Collections.unmodifiableList;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PREFIX;
import static org.apache.geode.distributed.internal.DistributionConfig.SSL_SYSTEM_PROPS_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.SYS_PROP_NAME;
import static org.apache.geode.internal.util.ArrayUtils.asList;

import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Immutable;

public class ArgumentRedactor {

  public static final String REDACTED = "********";

  /**
   * Taboo for an argument (option=argument) to contain this list of strings.
   */
  @Immutable
  private static final List<String> TABOO_TO_CONTAIN = unmodifiableList(asList("password"));

  /**
   * Taboo for an option (option=argument) to contain this list of strings.
   */
  @Immutable
  private static final List<String> TABOO_FOR_OPTION_TO_START_WITH =
      unmodifiableList(asList(SYS_PROP_NAME, SSL_SYSTEM_PROPS_NAME, SECURITY_PREFIX));

  private ArgumentRedactor() {
    // do not instantiate
  }

  /**
   * Parse a string to find option/argument pairs and redact the arguments if necessary.<br>
   *
   * The following format is expected:<br>
   * - Each option/argument pair should be separated by spaces.<br>
   * - The option of each pair must be preceded by at least one hyphen '-'.<br>
   * - Arguments may or may not be wrapped in quotation marks.<br>
   * - Options and arguments may be separated by an equals sign '=' or any number of spaces.<br>
   * <br>
   * Examples:<br>
   * "--password=secret"<br>
   * "--user me --password secret"<br>
   * "-Dflag -Dopt=arg"<br>
   * "--classpath=."<br>
   *
   * See {@link ArgumentRedactorRegex} for more information on the regular expression used.
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

    Matcher matcher = ArgumentRedactorRegex.getPattern().matcher(line);
    while (matcher.find()) {
      String option = matcher.group(2);
      if (!isTaboo(option)) {
        continue;
      }

      String leadingBoundary = matcher.group(1);
      String separator = matcher.group(3);
      String withRedaction = leadingBoundary + option + separator + REDACTED;
      line = line.replace(matcher.group(), withRedaction);
    }

    if (wasPaddedWithHyphen) {
      line = line.substring(1);
    }
    return line;
  }

  /**
   * Alias for {@code redact(line, true)}. See
   * {@link org.apache.geode.internal.util.ArgumentRedactor#redact(java.lang.String, boolean)}
   */
  public static String redact(String line) {
    return redact(line, true);
  }

  public static String redact(final List<String> args) {
    return redact(String.join(" ", args));
  }

  /**
   * Return the redaction string if the provided option's argument should be redacted.
   * Otherwise, return the provided argument unchanged.
   *
   * @param option A string such as a system property, jvm parameter or command-line option.
   * @param argument A string that is the argument assigned to the option.
   *
   * @return A redacted string if the option indicates it should be redacted, otherwise the
   *         provided argument.
   */
  public static String redactArgumentIfNecessary(String option, String argument) {
    if (isTaboo(option)) {
      return REDACTED;
    }
    return argument;
  }

  /**
   * Determine whether a option's argument should be redacted.
   *
   * @param option The option in question.
   *
   * @return true if the value should be redacted, otherwise false.
   */
  static boolean isTaboo(String option) {
    if (option == null) {
      return false;
    }
    for (String taboo : TABOO_FOR_OPTION_TO_START_WITH) {
      // If a parameter is passed with -Dsecurity-option=argument, the option option is
      // "Dsecurity-option".
      // With respect to taboo words, also check for the addition of the extra D
      if (option.toLowerCase().startsWith(taboo) || option.toLowerCase().startsWith("d" + taboo)) {
        return true;
      }
    }
    for (String taboo : TABOO_TO_CONTAIN) {
      if (option.toLowerCase().contains(taboo)) {
        return true;
      }
    }
    return false;
  }

  public static List<String> redactEachInList(List<String> argList) {
    return argList.stream().map(ArgumentRedactor::redact).collect(Collectors.toList());
  }
}
