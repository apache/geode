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

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * Validates that mandatory @ShellOption parameters are provided before command execution.
 *
 * <p>
 * In Spring Shell 3.x, when commands are executed programmatically (e.g., via GfshParserRule),
 * the shell allows NULL values for parameters that don't have a defaultValue specified.
 * This interceptor restores Spring Shell 1.x behavior by validating mandatory parameters
 * and returning "Invalid command" error before the command method executes.
 *
 * <p>
 * A parameter is considered mandatory if:
 * <ul>
 * <li>It has a @ShellOption annotation</li>
 * <li>Its defaultValue is null, empty, "__NONE__", or ShellOption.NULL</li>
 * </ul>
 *
 * <p>
 * Usage: Apply to command methods via @CliMetaData:
 *
 * <pre>
 * &#64;ShellMethod(key = "my-command")
 * &#64;CliMetaData(
 *     interceptor = "org.apache.geode.management.internal.cli.MandatoryParameterValidationInterceptor")
 * public ResultModel execute(&#64;ShellOption("id") String id) {
 *   // id is guaranteed to be non-null when method executes
 * }
 * </pre>
 *
 * <p>
 * <b>Note:</b> This class is instantiated via reflection by the CLI framework.
 * Do NOT add Spring annotations like @Component - they will not work in this context.
 */
public class MandatoryParameterValidationInterceptor extends AbstractCliAroundInterceptor {

  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {
    Method method = parseResult.getMethod();
    Parameter[] parameters = method.getParameters();
    Object[] arguments = parseResult.getArguments();

    for (int i = 0; i < parameters.length; i++) {
      ShellOption option = parameters[i].getAnnotation(ShellOption.class);
      Object value = arguments[i];

      if (option != null) {
        boolean required = isRequired(option);

        if (required && isNullOrEmpty(value)) {
          // Return "Invalid command" to match Spring Shell 1.x behavior
          return ResultModel.createError("Invalid command");
        }
      }
    }

    // Validation passed - allow command to execute
    return ResultModel.createInfo("");
  }

  /**
   * Determines if a @ShellOption parameter is required (mandatory).
   *
   * A parameter is required if it doesn't have a default value or if the default value
   * is the NULL marker.
   *
   * @param option the ShellOption annotation
   * @return true if the parameter is required, false otherwise
   */
  private boolean isRequired(ShellOption option) {
    String defaultValue = option.defaultValue();
    // Required if no default value, empty, or is the NULL marker
    // "__NONE__" is Spring Shell's marker for no default value
    return defaultValue == null || defaultValue.isEmpty() || "__NONE__".equals(defaultValue)
        || ShellOption.NULL.equals(defaultValue);
  }

  /**
   * Checks if a value is null or empty (for String parameters).
   *
   * @param value the parameter value to check
   * @return true if value is null or empty string, false otherwise
   */
  private boolean isNullOrEmpty(Object value) {
    if (value == null) {
      return true;
    }
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      // Check for empty string, literal "null", or the OPTION_NOT_VALUED sentinel
      // The sentinel might have "/" prefix from region path conversion
      return strValue.isEmpty() || "null".equals(strValue)
          || strValue.equals("__OPTION_NOT_VALUED__")
          || strValue.equals("/__OPTION_NOT_VALUED__");
    }
    return false;
  }
}
