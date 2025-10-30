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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogLevelConverter}.
 *
 * SPRING SHELL 3.x MIGRATION:
 * - Spring Shell 1.x: Converter.getAllPossibleValues() for auto-completion
 * - Spring Shell 3.x: ValueProvider for auto-completion (separate from conversion)
 * - Spring Shell 1.x: Converter.supports() for type matching
 * - Spring Shell 3.x: Type safety via Converter<String, String> generics
 *
 * Migration Changes:
 * - Removed getAllPossibleValues() tests (completion now uses ValueProvider)
 * - Removed supports() tests (not part of Spring 3.x Converter interface)
 * - Added convert() tests to verify log level validation
 * - Retained getCompletionValues() for internal use by ValueProviders
 */
@Category({GfshTest.class, LoggingTest.class})
public class LogLevelConverterTest {

  private LogLevelConverter converter;

  @Before
  public void setUp() {
    converter = new LogLevelConverter();
  }

  @Test
  public void getCompletionValuesContainsAllLog4jLevels() {
    // SPRING SHELL 3.x: getCompletionValues() used internally by ValueProvider
    Set<String> completionValues = converter.getCompletionValues();

    // Log4j 2 has 8 levels: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
    assertThat(completionValues).hasSize(8);

    // Verify all values are valid Log4j levels
    for (String levelName : completionValues) {
      assertThat(Level.getLevel(levelName)).isNotNull();
    }

    // Verify specific levels are present
    assertThat(completionValues).contains("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR",
        "FATAL", "OFF");
  }

  @Test
  public void convertValidLogLevels() {
    // SPRING SHELL 3.x: convert() method validates and returns log level string
    assertThat(converter.convert("INFO")).isEqualTo("INFO");
    assertThat(converter.convert("DEBUG")).isEqualTo("DEBUG");
    assertThat(converter.convert("WARN")).isEqualTo("WARN");
    assertThat(converter.convert("ERROR")).isEqualTo("ERROR");
    assertThat(converter.convert("FATAL")).isEqualTo("FATAL");
    assertThat(converter.convert("TRACE")).isEqualTo("TRACE");
    assertThat(converter.convert("ALL")).isEqualTo("ALL");
    assertThat(converter.convert("OFF")).isEqualTo("OFF");
  }

  @Test
  public void convertInvalidLogLevelThrowsException() {
    // Invalid log level should throw IllegalArgumentException
    assertThatThrownBy(() -> converter.convert("INVALID"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid log level: INVALID");
  }

  @Test
  public void convertEmptyStringThrowsException() {
    assertThatThrownBy(() -> converter.convert(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid log level");
  }
}
