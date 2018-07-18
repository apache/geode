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

package org.apache.geode.internal.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.junit.Test;

import org.apache.geode.internal.logging.InternalLogWriter;

public class LogLevelTest {
  @Test
  public void testGetLevel() {
    assertThat(LogLevel.getLevel("all")).isEqualTo(Level.ALL);
    assertThat(LogLevel.getLevel("fatal")).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel("severe")).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel("error")).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel("warn")).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel("warning")).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel("info")).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel("config")).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel("debug")).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel("fine")).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel("finer")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel("finest")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel("trace")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel("all")).isEqualTo(Level.ALL);
    assertThat(LogLevel.getLevel("none")).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel("off")).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel("notrecognizable")).isNull();
  }

  @Test
  public void testResolveLevel() {
    assertThat(LogLevel.resolveLevel("all")).isEqualTo(Level.ALL);
    assertThat(LogLevel.resolveLevel("fatal")).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel("severe")).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel("error")).isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel("warn")).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel("warning")).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel("info")).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel("config")).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel("debug")).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel("fine")).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel("finer")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel("finest")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel("trace")).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel("all")).isEqualTo(Level.ALL);
    assertThat(LogLevel.resolveLevel("none")).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel("off")).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel("notrecognizable")).isEqualTo(Level.OFF);
  }

  @Test
  public void testGetLog4jLevel() throws Exception {
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.SEVERE_LEVEL)).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.ERROR_LEVEL)).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.WARNING_LEVEL)).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.CONFIG_LEVEL)).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.INFO_LEVEL)).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.FINE_LEVEL)).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.FINER_LEVEL)).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.FINEST_LEVEL)).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.ALL_LEVEL)).isEqualTo(Level.ALL);
    assertThat(LogLevel.getLog4jLevel(InternalLogWriter.NONE_LEVEL)).isEqualTo(Level.OFF);
    assertThatThrownBy(() -> LogLevel.getLog4jLevel(123123123))
        .hasMessageContaining("Unknown LogWriter level");
  }

  @Test
  public void testGetLogWriterLevel() throws Exception {
    assertThat(LogLevel.getLogWriterLevel(Level.FATAL)).isEqualTo(InternalLogWriter.SEVERE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.ERROR)).isEqualTo(InternalLogWriter.ERROR_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.WARN)).isEqualTo(InternalLogWriter.WARNING_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.INFO)).isEqualTo(InternalLogWriter.INFO_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.DEBUG)).isEqualTo(InternalLogWriter.FINE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.TRACE)).isEqualTo(InternalLogWriter.FINEST_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.ALL)).isEqualTo(InternalLogWriter.ALL_LEVEL);
    assertThat(LogLevel.getLogWriterLevel(Level.OFF)).isEqualTo(InternalLogWriter.NONE_LEVEL);
  }

  @Test
  public void testGetLogWriterLevelWithString() throws Exception {
    assertThat(LogLevel.getLogWriterLevel("FATAL")).isEqualTo(InternalLogWriter.SEVERE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("error")).isEqualTo(InternalLogWriter.ERROR_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("WARN")).isEqualTo(InternalLogWriter.WARNING_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("info")).isEqualTo(InternalLogWriter.INFO_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("CONFIG")).isEqualTo(InternalLogWriter.CONFIG_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("DEBUG")).isEqualTo(InternalLogWriter.FINE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("trace")).isEqualTo(InternalLogWriter.FINEST_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("ALL")).isEqualTo(InternalLogWriter.ALL_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("none")).isEqualTo(InternalLogWriter.NONE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("fine")).isEqualTo(InternalLogWriter.FINE_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("finer")).isEqualTo(InternalLogWriter.FINER_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("finest")).isEqualTo(InternalLogWriter.FINEST_LEVEL);
    assertThat(LogLevel.getLogWriterLevel("level-1234")).isEqualTo(1234);
    assertThatThrownBy(() -> LogLevel.getLogWriterLevel((String) null))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LogLevel.getLogWriterLevel("test"))
        .hasMessageContaining("OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL");
  }
}
