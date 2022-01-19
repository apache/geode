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
package org.apache.geode.management.internal.cli.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Inserts quotes around the values of any option values that begin with hyphen.
 */
public class HyphenFormatter {

  private static final String OPTION_PATTERN = "--[a-zA-Z]+[a-zA-Z\\-]*=*";

  private static final String QUOTE = "\"";
  private static final String EQUAL_HYPHEN = "=-";
  private static final String EQUAL = "=";
  private static final String SPACE = " ";

  private StringBuilder formatted;

  /**
   * Returns command with quotes around the values of any option values that begin with hyphen.
   */
  public String formatCommand(String command) {
    if (!containsOption(command)) {
      return command;
    }
    formatted = new StringBuilder();

    List<String> strings = split(command);
    for (String string : strings) {
      if (string.contains(EQUAL_HYPHEN)) {
        int indexOfEquals = string.indexOf(EQUAL);
        formatted.append(string, 0, indexOfEquals + 1);
        formatted.append(QUOTE);
        formatted.append(string.substring(indexOfEquals + 1));
        formatted.append(QUOTE);
      } else {
        formatted.append(string);
      }
      formatted.append(SPACE);
    }
    return formatted.toString().trim();
  }

  /**
   * Returns true if command contains any options.
   */
  boolean containsOption(String cmd) {
    return Pattern.compile(OPTION_PATTERN).matcher(cmd).find();
  }

  private List<String> split(String cmd) {
    List<String> strings = new ArrayList<>();
    Matcher matcher = Pattern.compile(OPTION_PATTERN).matcher(cmd);

    int index = 0; // first index of --option=

    while (matcher.find()) {
      if (matcher.start() > index) {
        String option = cmd.substring(index, matcher.start()).trim();
        strings.add(option);
      }
      index = matcher.start();
    }

    String lastOne = cmd.substring(index);
    if (lastOne != null) {
      strings.add(lastOne);
    }

    return strings;
  }
}
