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

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;

public class ArgumentRedactor {
  public static final String redacted = "********";

  @Immutable
  private static final List<String> tabooToContain =
      Collections.unmodifiableList(ArrayUtils.asList("password"));
  @Immutable
  private static final List<String> tabooForOptionToStartWith =
      Collections.unmodifiableList(ArrayUtils.asList(DistributionConfig.SYS_PROP_NAME,
          DistributionConfig.SSL_SYSTEM_PROPS_NAME,
          ConfigurationProperties.SECURITY_PREFIX));

  private static final Pattern optionWithArgumentPattern = getOptionWithArgumentPattern();


  /**
   * This method returns the {@link java.util.regex.Pattern} given below, used to capture
   * command-line options that accept an argument. For clarity, the regex is given here without
   * the escape characters required by Java's string handling.
   * <p>
   *
   * {@code ((?:^| )(?:--J=)?--?)([^\s=]+)(?=[ =])( *[ =] *)(?! *-)((?:"[^"]*"|\S+))}
   *
   * <p>
   * This pattern consists of one captured boundary,
   * three additional capture groups, and two look-ahead boundaries.
   *
   * <p>
   * The four capture groups are:
   * <ul>
   * <li>[1] The beginning boundary, including at most one leading space,
   * possibly including "--J=", and including the option's leading "-" or "--"</li>
   * <li>[2] The option, which cannot include spaces</li>
   * <li>[3] The option / argument separator, consisting of at least one character
   * made of spaces and/or at most one "="</li>
   * <li>[4] The argument, which terminates at the next space unless it is encapsulated by
   * quotation-marks, in which case it terminates at the next quotation mark.</li>
   * </ul>
   *
   * Look-ahead groups avoid falsely identifying two flag options (e.g. `{@code --help --all}`) from
   * interpreting the second flag as the argument to the first option
   * (here, misinterpreting as `{@code --help="--all"}`).
   * <p>
   *
   * Note that at time of writing, the argument (capture group 4) is not consumed by this class's
   * logic, but its capture has proven repeatedly useful during iteration and testing.
   */
  private static Pattern getOptionWithArgumentPattern() {
    String capture_beginningBoundary;
    {
      String spaceOrBeginningAnchor = "(?:^| )";
      String maybeLeadingWithDashDashJEquals = "(?:--J=)?";
      String oneOrTwoDashes = "--?";
      capture_beginningBoundary =
          "(" + spaceOrBeginningAnchor + maybeLeadingWithDashDashJEquals + oneOrTwoDashes + ")";
    }

    String capture_optionNameHasNoSpaces = "([^\\s=]+)";

    String boundary_lookAheadForSpaceOrEquals = "(?=[ =])";

    String capture_optionArgumentSeparator = "( *[ =] *)";

    String boundary_negativeLookAheadToPreventNextOptionAsThisArgument = "(?! *-)";

    String capture_Argument;
    {
      String argumentCanBeAnythingBetweenQuotes = "\"[^\"]*\"";
      String argumentCanHaveNoSpacesWithoutQuotes = "\\S+";
      String argumentCanBeEitherOfTheAbove = "(?:" + argumentCanBeAnythingBetweenQuotes + "|"
          + argumentCanHaveNoSpacesWithoutQuotes + ")";
      capture_Argument = "(" + argumentCanBeEitherOfTheAbove + ")";
    }

    String fullPattern = capture_beginningBoundary + capture_optionNameHasNoSpaces
        + boundary_lookAheadForSpaceOrEquals + capture_optionArgumentSeparator
        + boundary_negativeLookAheadToPreventNextOptionAsThisArgument + capture_Argument;
    return Pattern.compile(fullPattern);
  }

  private ArgumentRedactor() {}

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
   * See {@link #getOptionWithArgumentPattern()} for more information on
   * the regular expression used.
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

    Matcher matcher = optionWithArgumentPattern.matcher(line);
    while (matcher.find()) {
      String option = matcher.group(2);
      if (!isTaboo(option)) {
        continue;
      }

      String leadingBoundary = matcher.group(1);
      String separator = matcher.group(3);
      String withRedaction = leadingBoundary + option + separator + redacted;
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
      return redacted;
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
    for (String taboo : tabooForOptionToStartWith) {
      // If a parameter is passed with -Dsecurity-option=argument, the option option is
      // "Dsecurity-option".
      // With respect to taboo words, also check for the addition of the extra D
      if (option.toLowerCase().startsWith(taboo) || option.toLowerCase().startsWith("d" + taboo)) {
        return true;
      }
    }
    for (String taboo : tabooToContain) {
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
