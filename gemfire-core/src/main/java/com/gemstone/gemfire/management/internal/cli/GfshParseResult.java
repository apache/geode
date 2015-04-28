/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.internal.cli.shell.GfshExecutionStrategy;
import com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker;

/**
 * Immutable representation of the outcome of parsing a given shell line. *
 * Extends {@link ParseResult} to add a field to specify the command string that
 * was input by the user.
 * 
 * <p>
 * Some commands are required to be executed on a remote GemFire managing
 * member. These should be marked with the annotation
 * {@link CliMetaData#shellOnly()} set to <code>false</code>.
 * {@link GfshExecutionStrategy} will detect whether the command is a remote
 * command and send it to ManagementMBean via {@link OperationInvoker}.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class GfshParseResult extends ParseResult {
  private String userInput;
  private String commandName;
  private Map<String, String> paramValueStringMap;

  /**
   * Creates a GfshParseResult instance to represent parsing outcome.
   * 
   * @param method
   *          Method associated with the command
   * @param instance
   *          Instance on which this method has to be executed
   * @param arguments
   *          arguments of the method
   * @param userInput
   *          user specified commands string
   */
  protected GfshParseResult(final Method method, final Object instance, 
                            final Object[] arguments, final String userInput, 
                            final String commandName,
                            final Map<String, String> parametersAsString) {
    super(method, instance, arguments);
    this.userInput = userInput;
    this.commandName = commandName;
    this.paramValueStringMap = new HashMap<String, String>(parametersAsString);
  }
  
  /**
   * @return the userInput
   */
  public String getUserInput() {
    return userInput;
  }
  

  /**
   * @return the unmodifiable paramValueStringMap
   */
  public Map<String, String> getParamValueStrings() {
    return Collections.unmodifiableMap(paramValueStringMap);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(GfshParseResult.class.getSimpleName());
    builder.append(" [method=").append(getMethod());
    builder.append(", instance=").append(getInstance());
    builder.append(", arguments=").append(CliUtil.arrayToString(getArguments()));
    builder.append("]");
    return builder.toString();
  }

  public String getCommandName() {
    return commandName;
  }
}
