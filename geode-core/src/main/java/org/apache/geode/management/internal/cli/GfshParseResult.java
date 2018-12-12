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

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.shell.GfshExecutionStrategy;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;

/**
 * Immutable representation of the outcome of parsing a given shell line. * Extends
 * {@link ParseResult} to add a field to specify the command string that was input by the user.
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
public class GfshParseResult extends ParseResult {
  private String userInput;
  private String commandName;
  private Map<String, Object> paramValueMap = new HashMap<>();

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
    super(method, instance, arguments);
    this.userInput = userInput.trim();

    CliCommand cliCommand = method.getAnnotation(CliCommand.class);
    commandName = cliCommand.value()[0];

    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    if (arguments == null) {
      return;
    }

    for (int i = 0; i < arguments.length; i++) {
      Object argument = arguments[i];
      if (argument == null) {
        continue;
      }

      CliOption cliOption = getCliOption(parameterAnnotations, i);

      // this maps are used for easy access of option values.
      // It's used in tests and validation of option values in pre-execution
      paramValueMap.put(cliOption.key()[0], argument);

      String argumentAsString;
      if (argument instanceof Object[]) {
        argumentAsString = StringUtils.join((Object[]) argument, ",");
      } else {
        argumentAsString = argument.toString();
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

  private CliOption getCliOption(Annotation[][] parameterAnnotations, int index) {
    Annotation[] annotations = parameterAnnotations[index];
    for (Annotation annotation : annotations) {
      if (annotation instanceof CliOption) {
        return (CliOption) annotation;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return this.userInput;
  }
}
