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

import static org.apache.geode.internal.util.ArgumentRedactorRegex.Group.ARGUMENT;
import static org.apache.geode.internal.util.ArgumentRedactorRegex.Group.ASSIGNMENT;
import static org.apache.geode.internal.util.ArgumentRedactorRegex.Group.OPTION;
import static org.apache.geode.internal.util.ArgumentRedactorRegex.Group.PREFIX;

import java.util.regex.Pattern;

/**
 * Regex with named capture groups that can be used to match strings containing an
 * option with an argument value.
 *
 * <p>
 * This method returns the {@link Pattern} given below, used to capture
 * command-line options that accept an argument. For clarity, the regex is given here without
 * the escape characters required by Java's string handling.
 *
 * <p>
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
 * <p>
 * Look-ahead groups avoid falsely identifying two flag options (e.g. `{@code --help --all}`) from
 * interpreting the second flag as the argument to the first option
 * (here, misinterpreting as `{@code --help="--all"}`).
 *
 * <p>
 * Note that at time of writing, the argument (capture group 4) is not consumed by this class's
 * logic, but its capture has proven repeatedly useful during iteration and testing.
 */
public class ArgumentRedactorRegex {

  private static final String REGEX =
      "" + PREFIX + OPTION + Boundary.SPACE_OR_EQUALS + ASSIGNMENT +
          Boundary.NEGATIVE_TO_PREVENT_NEXT_OPTION_AS_THIS_ARGUMENT + ARGUMENT;
  // private static final String REGEX = "" + PREFIX + OPTION + SPACE_OR_EQUALS + ASSIGNMENT +
  // ARGUMENT;

  private static final Pattern PATTERN = Pattern.compile(REGEX);

  public static String getRegex() {
    return REGEX;
  }

  public static Pattern getPattern() {
    return PATTERN;
  }

  private static final String beginningOfLineOrSpace = "(?:^| )";
  private static final String optionalTwoDashesJEquals = "(?:--J=)?";
  private static final String oneOrTwoDashes = "--?";
  private static final String noSpaces = "[^\\s=]+";
  // private static final String equalsWithOptionalSpaces = " *[ =] *";
  private static final String equalsWithOptionalSpaces = " *= *";
  private static final String anythingBetweenQuotes = "\"[^\"]*\"";
  // private static final String anythingBetweenQuotes = "[^\\s\"']+|\"([^\"]*)\"|'([^']*)'";
  private static final String noSpacesWithoutQuotes = "\\S+";

  public enum Group {
    PREFIX(1, "prefix",
        "(?<prefix>" + beginningOfLineOrSpace + optionalTwoDashesJEquals + oneOrTwoDashes + ")"),
    OPTION(2, "option",
        "(?<option>" + noSpaces + ")"),
    ASSIGNMENT(3, "assignment",
        "(?<assignment>" + equalsWithOptionalSpaces + ")"),
    ARGUMENT(4, "argument",
        "(?<argument>(?:" + anythingBetweenQuotes + "|" + noSpacesWithoutQuotes + "))");
    // ARGUMENT(4, "argument", "(?<argument>" + anythingBetweenQuotes + ")");

    private final int index;
    private final String name;
    private final String regex;

    Group(final int index, final String name, final String regex) {
      this.index = index;
      this.name = name;
      this.regex = regex;
    }

    public int getIndex() {
      return index;
    }

    public String getName() {
      return name;
    }

    public String getRegex() {
      return regex;
    }

    @Override
    public String toString() {
      return regex;
    }
  }

  public enum Boundary {
    SPACE_OR_EQUALS("(?=[ =])"),
    // NEGATIVE_TO_PREVENT_NEXT_OPTION_AS_THIS_ARGUMENT("(?! *-)");
    NEGATIVE_TO_PREVENT_NEXT_OPTION_AS_THIS_ARGUMENT("(?=-?)");

    private final String regex;

    Boundary(final String regex) {
      this.regex = regex;
    }

    public String getRegex() {
      return regex;
    }

    @Override
    public String toString() {
      return regex;
    }
  }
}
