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

import static org.apache.geode.internal.util.redaction.ParserRegex.Group.ASSIGN;
import static org.apache.geode.internal.util.redaction.ParserRegex.Group.KEY;
import static org.apache.geode.internal.util.redaction.ParserRegex.Group.PREFIX;
import static org.apache.geode.internal.util.redaction.ParserRegex.Group.VALUE;

import java.util.regex.Pattern;

/**
 * Regex with named capture groups that can be used to match strings containing a key with value or
 * a flag.
 *
 * <p>
 * The raw regex string is:
 *
 * {@code (?<prefix>--J=-D|-D|--)(?<key>[^\s=]+)(?:(?! (?:--J=-D|-D|--))(?<assign> *[ =] *)(?! (?:--J=-D|-D|--))(?<value>"[^"]*"|\S+))?}
 */
class ParserRegex {

  private static final String REGEX =
      String.valueOf(PREFIX) +
          KEY +
          "(?:(?! (?:--J=-D|-D|--))" +
          ASSIGN + "(?! (?:--J=-D|-D|--))" +
          VALUE +
          ")?";

  private static final Pattern PATTERN = Pattern.compile(REGEX);

  static String getRegex() {
    return REGEX;
  }

  static Pattern getPattern() {
    return PATTERN;
  }

  enum Group {
    /** Prefix precedes each key and may be --, -D, or --J=-D */
    PREFIX(1, "prefix", "(?<prefix>--J=-D|-D|--)"),

    /** Key has a value or represents a flag when no value is assigned */
    KEY(2, "key", "(?<key>[^\\s=]+)"),

    /** Assign is an operator for assigning a value to a key and may be = or space */
    ASSIGN(3, "assign", "(?<assign> *[ =] *)"),

    /** Value is assigned to a key following an assign operator */
    VALUE(4, "value", "(?<value>\"[^\"]*\"|\\S+)");

    private final int index;
    private final String name;
    private final String regex;

    Group(final int index, final String name, final String regex) {
      this.index = index;
      this.name = name;
      this.regex = regex;
    }

    int getIndex() {
      return index;
    }

    String getName() {
      return name;
    }

    String getRegex() {
      return regex;
    }

    @Override
    public String toString() {
      return regex;
    }
  }
}
