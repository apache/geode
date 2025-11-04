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
package org.apache.geode.management.internal.cli.converters;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * Spring Shell 3.x converter for Log4j log levels.
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * This converter provides string-to-string pass-through conversion for log levels.
 * Spring Shell 1.x used this primarily for auto-completion; Spring Shell 3.x
 * uses ValueProvider for completion instead.
 *
 * <p>
 * Valid log levels: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
 *
 * @since GemFire 7.0
 */
@Component
public class LogLevelConverter implements Converter<String, String> {
  private final Set<String> logLevels;

  public LogLevelConverter() {
    logLevels = Arrays.stream(Level.values()).map(Level::name).collect(Collectors.toSet());
  }

  /**
   * Returns the set of valid log level names for validation.
   *
   * @return set of log level names (ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF)
   */
  public Set<String> getCompletionValues() {
    return logLevels;
  }

  /**
   * Converts and validates a log level string.
   *
   * @param source the log level name (e.g., "INFO", "DEBUG")
   * @return the validated log level string
   * @throws IllegalArgumentException if the log level is invalid
   */
  @Override
  public String convert(@NonNull String source) {
    // Validate that the log level exists
    if (!logLevels.contains(source.toUpperCase())) {
      throw new IllegalArgumentException(
          "Invalid log level: " + source + ". Valid levels: " + logLevels);
    }
    return source;
  }
}
