/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.util;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.parser.SyntaxConstants;


/**-
 * Helper class to build command strings, used in the Dunits for testing gfsh
 * commands
 * 
 * 
 * @since GemFire 7.0
 */
public class CommandStringBuilder {
  private final String OPTION_MARKER    = SyntaxConstants.LONG_OPTION_SPECIFIER;
  private final String EQUAL_TO         = SyntaxConstants.OPTION_VALUE_SPECIFIER;
  private final String ARG_SEPARATOR    = SyntaxConstants.OPTION_SEPARATOR;
  private final String OPTION_SEPARATOR = SyntaxConstants.OPTION_SEPARATOR;
  private final String SINGLE_SPACE     = " ";

  private final    StringBuffer buffer;
  private volatile boolean      hasOptions;

  public CommandStringBuilder(String command) {
    buffer = new StringBuffer(command);
  }

  public CommandStringBuilder addArgument(String argument) {
    if (hasOptions) {
      throw new IllegalStateException("Arguments can't be specified after options. Built String is: "+buffer.toString());
    }
    buffer.append(ARG_SEPARATOR);
    buffer.append(argument);
    return this;
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
  
  public CommandStringBuilder addOptionWithValueCheck(String option, String value) {
    if (!StringUtils.isBlank(value)){
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
    buffer.append(SyntaxConstants.CONTINUATION_CHARACTER);
    buffer.append(getLineSeparator());
    return this;
  }

  private static String getLineSeparator() {
    // Until TestableGfsh issue #46388 is resolved
    if (SystemUtils.isWindows()) {
      return "\r";
    } else {
      return GfshParser.LINE_SEPARATOR;
    }
  }

  public String getCommandString() {
    return buffer.toString();
  }

  @Override
  public String toString() {
    return getCommandString();
  }
}
