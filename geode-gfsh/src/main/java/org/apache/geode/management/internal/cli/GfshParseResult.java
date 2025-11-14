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
package org.apache.geode.management.internal.cli;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.shell.GfshExecutionStrategy;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;

/**
 * Immutable representation of the outcome of parsing a given shell line.
 * Standalone class (no longer extends Spring Shell's ParseResult which was removed in Shell 3.x).
 *
 * <p>
 * Some commands are required to be executed on a remote GemFire managing member. These should be
 * marked with the annotation {@link CliMetaData#shellOnly()} set to <code>false</code>.
 * {@link GfshExecutionStrategy} will detect whether the command is a remote command and send it to
 * ManagementMBean via {@link OperationInvoker}.
 *
 *
 * @since GemFire 7.0
 */
public class GfshParseResult {
  private String userInput;
  private final String commandName;
  private final Map<String, Object> paramValueMap = new HashMap<>();

  // Fields previously inherited from ParseResult:
  private final Method method;
  private final Object instance;
  private final Object[] arguments;

  /**
   * Creates a GfshParseResult instance to represent parsing outcome.
   *
   * @param method Method associated with the command
   * @param instance Instance on which this method has to be executed
   * @param arguments arguments of the method
   * @param userInput user specified commands string
   */
  protected GfshParseResult(final Method method, final Object instance, final Object[] arguments,
      final String userInput) {
    this.method = method;
    this.instance = instance;
    this.arguments = arguments;
    this.userInput = userInput.trim();

    // Spring Shell 3.x uses @ShellMethod instead of @Command
    org.springframework.shell.standard.ShellMethod shellMethod =
        method.getAnnotation(org.springframework.shell.standard.ShellMethod.class);
    if (shellMethod != null && shellMethod.key().length > 0) {
      commandName = shellMethod.key()[0];
    } else {
      commandName = method.getName();
    }

    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    if (arguments == null) {
      return;
    }

    for (int i = 0; i < arguments.length; i++) {
      Object argument = arguments[i];

      ShellOption option = getOption(parameterAnnotations, i);
      if (option == null) {
        // Parameter without @ShellOption annotation - skip it
        continue;
      }

      // Store in map using the first value as key (not longNames, which doesn't exist in Shell 3.x)
      // This map is used for easy access of option values in tests and validation
      String optionName = option.value().length > 0 ? option.value()[0] : null;
      if (optionName != null) {
        paramValueMap.put(optionName, argument);
      }

      // Note: argumentAsString was previously computed but not used
      // Keeping the pattern for potential future use
      if (argument != null) {
        String argumentAsString;
        if (argument instanceof Object[]) {
          argumentAsString = StringUtils.join((Object[]) argument, ",");
        } else {
          argumentAsString = argument.toString();
        }
      }
    }
  }

  /**
   * @return the userInput
   */
  public String getUserInput() {
    return userInput;
  }

  public void setUserInput(String userText) {
    userInput = userText;
  }

  public Object getParamValue(String param) {
    return paramValueMap.get(param);
  }


  public String getParamValueAsString(String param) {
    Object argument = paramValueMap.get(param);
    if (argument == null) {
      return null;
    }

    String argumentAsString;
    if (argument instanceof Object[]) {
      argumentAsString = StringUtils.join((Object[]) argument, ",");
    } else {
      argumentAsString = argument.toString();
    }
    return argumentAsString;
  }

  public String getCommandName() {
    return commandName;
  }

  // Getters for fields previously inherited from ParseResult
  public Method getMethod() {
    return method;
  }

  public Object getInstance() {
    return instance;
  }

  public Object[] getArguments() {
    return arguments;
  }

  private ShellOption getOption(Annotation[][] parameterAnnotations, int index) {
    Annotation[] annotations = parameterAnnotations[index];
    for (Annotation annotation : annotations) {
      if (annotation instanceof ShellOption) {
        return (ShellOption) annotation;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return userInput;
  }
}
