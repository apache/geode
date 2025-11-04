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
package org.apache.geode.management.internal.cli.completion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;

/**
 * Provides completion for log level values when the parameter name matches "loglevel".
 *
 * REASONING: Shell 1.x provided log level completions for the --loglevel parameter in
 * the "change loglevel" command. This provider maintains backward compatibility by
 * detecting the parameter name and offering standard Log4j2 log levels.
 *
 * Supports parameter types: String (when parameter name contains "loglevel")
 *
 * Log levels offered (in order): ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
 * These match the 8 log levels expected by testCompleteLogLevel and
 * testCompleteLogLevelWithEqualSign.
 *
 * @since Spring Shell 3.x migration
 */
public class LogLevelCompletionProvider implements ValueCompletionProvider {

  /**
   * Standard Log4j2 log levels, ordered from most verbose to least verbose.
   * ALL logs everything, OFF logs nothing.
   */
  private static final List<String> LOG_LEVELS = Arrays.asList(
      "ALL", // Log everything
      "TRACE", // Very detailed, typically only for diagnosing problems
      "DEBUG", // Detailed, useful for debugging
      "INFO", // Informational messages highlighting application progress
      "WARN", // Potentially harmful situations
      "ERROR", // Error events that might still allow app to continue
      "FATAL", // Severe error events that presumably lead app to abort
      "OFF" // Turn off all logging
  );

  @Override
  public boolean supports(Class<?> targetType) {
    // This provider handles String parameters, but only if the context
    // indicates it's a log level parameter (checked in getCompletions)
    return String.class.equals(targetType);
  }

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    List<Completion> completions = new ArrayList<>();

    // REASONING: Only provide log levels if this is actually a loglevel parameter.
    // Check the option name from the context to avoid polluting all String parameters.
    // Context optionName includes dashes, e.g., "--loglevel"
    String optionName = context.getOptionName();
    if (optionName == null || !optionName.toLowerCase().contains("loglevel")) {
      return completions; // Not a loglevel parameter, return empty
    }

    // Filter log levels based on partial input (case-insensitive)
    String partial = (partialValue == null) ? "" : partialValue.toUpperCase();

    for (String level : LOG_LEVELS) {
      if (level.startsWith(partial)) {
        completions.add(new Completion(level));
      }
    }

    return completions;
  }
}
