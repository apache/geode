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

import java.io.File;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.management.internal.cli.GfshParser;


/**
 * - Helper class to build command strings, used in the Dunits for testing gfsh commands
 *
 * @since GemFire 7.0
 */
public class CommandStringBuilder {
  private static final String OPTION_MARKER = GfshParser.LONG_OPTION_SPECIFIER;
  private static final String EQUAL_TO = GfshParser.OPTION_VALUE_SPECIFIER;
  private static final String ARG_SEPARATOR = GfshParser.OPTION_SEPARATOR;
  private static final String OPTION_SEPARATOR = GfshParser.OPTION_SEPARATOR;
  private static final String SINGLE_SPACE = " ";
  private static final String DOUBLE_QUOTE = "\"";

  private final StringBuilder buffer;
  private volatile boolean hasOptions;

  public CommandStringBuilder(String command) {
    buffer = new StringBuilder(command);
  }

  private static String getLineSeparator() {
    // Until TestableGfsh issue #46388 is resolved
    if (SystemUtils.isWindows()) {
      return "\r";
    } else {
      return GfshParser.LINE_SEPARATOR;
    }
  }

  public CommandStringBuilder addOption(String option, String value) {
    buffer.append(OPTION_SEPARATOR);
    buffer.append(OPTION_MARKER);
    buffer.append(option);
    buffer.append(EQUAL_TO);
    buffer.append(value);
    hasOptions = true;
    return this;
  }

  public CommandStringBuilder addOption(String option, File value) {
    return addOption(option, quoteArgument(value.getAbsolutePath()));
  }

  public CommandStringBuilder addOptionWithValueCheck(String option, String value) {
    if (StringUtils.isNotBlank(value)) {
      return addOption(option, value);
    }
    return this;
  }

  public CommandStringBuilder addOption(String option) {
    buffer.append(OPTION_SEPARATOR);
    buffer.append(OPTION_MARKER);
    buffer.append(option);
    hasOptions = true;
    return this;
  }

  public CommandStringBuilder addNewLine() {
    buffer.append(SINGLE_SPACE); // add a space before continuation char
    buffer.append(getLineSeparator());
    return this;
  }

  public String getCommandString() {
    return buffer.toString();
  }

  @Override
  public String toString() {
    return getCommandString();
  }

  private String quoteArgument(String argument) {
    if (!argument.startsWith(DOUBLE_QUOTE)) {
      argument = DOUBLE_QUOTE + argument;
    }

    if (!argument.endsWith(DOUBLE_QUOTE)) {
      argument = argument + DOUBLE_QUOTE;
    }

    return argument;
  }
}
