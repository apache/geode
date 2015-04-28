/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;


/**-
 * Helper class to build command strings, used in the Dunits for testing gfsh
 * commands
 * 
 * @author Saurabh Bansod
 * 
 * @since 7.0
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
