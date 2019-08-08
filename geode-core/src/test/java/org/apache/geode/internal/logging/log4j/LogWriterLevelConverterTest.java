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

import static org.apache.geode.logging.internal.log4j.LogWriterLevelConverter.fromLevel;
import static org.apache.geode.logging.internal.log4j.LogWriterLevelConverter.toLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.LogWriterLevelConverter;
import org.apache.geode.logging.spi.LogWriterLevel;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogWriterLevelConverter}.
 */
@Category(LoggingTest.class)
public class LogWriterLevelConverterTest {

  @Test
  public void toLevel_LogWriterLevelAll_returnsLevelAll() {
    assertThat(toLevel(LogWriterLevel.ALL)).isEqualTo(Level.ALL);
  }

  @Test
  public void toLevel_LogWriterLevelSevere_returnsLevelFatal() {
    assertThat(toLevel(LogWriterLevel.SEVERE)).isEqualTo(Level.FATAL);
  }

  @Test
  public void toLevel_LogWriterLevelError_returnsLevelFatal() {
    assertThat(toLevel(LogWriterLevel.ERROR)).isEqualTo(Level.ERROR);
  }

  @Test
  public void toLevel_LogWriterLevelWarning_returnsLevelFatal() {
    assertThat(toLevel(LogWriterLevel.WARNING)).isEqualTo(Level.WARN);
  }

  @Test
  public void toLevel_LogWriterLevelInfo_returnsLevelInfo() {
    assertThat(toLevel(LogWriterLevel.INFO)).isEqualTo(Level.INFO);
  }

  @Test
  public void toLevel_LogWriterLevelConfig_returnsLevelInfo() {
    assertThat(toLevel(LogWriterLevel.CONFIG)).isEqualTo(Level.INFO);
  }

  @Test
  public void toLevel_LogWriterLevelFine_returnsLevelInfo() {
    assertThat(toLevel(LogWriterLevel.FINE)).isEqualTo(Level.DEBUG);
  }

  @Test
  public void toLevel_LogWriterLevelFiner_returnsLevelInfo() {
    assertThat(toLevel(LogWriterLevel.FINER)).isEqualTo(Level.TRACE);
  }

  @Test
  public void toLevel_LogWriterLevelFinest_returnsLevelInfo() {
    assertThat(toLevel(LogWriterLevel.FINEST)).isEqualTo(Level.TRACE);
  }

  @Test
  public void toLevel_LogWriterLevelOff_returnsLevelFatal() {
    assertThat(toLevel(LogWriterLevel.NONE)).isEqualTo(Level.OFF);
  }

  @Test
  public void toLogWriterLevel_LevelAll_returnsLogWriterLevelAll() {
    assertThat(fromLevel(Level.ALL)).isEqualTo(LogWriterLevel.ALL);
  }

  @Test
  public void toLogWriterLevel_LevelFatal_returnsLogWriterLevelSevere() {
    assertThat(fromLevel(Level.FATAL)).isEqualTo(LogWriterLevel.SEVERE);
  }

  @Test
  public void toLogWriterLevel_LevelError_returnsLogWriterLevelError() {
    assertThat(fromLevel(Level.ERROR)).isEqualTo(LogWriterLevel.ERROR);
  }

  @Test
  public void toLogWriterLevel_LevelWarn_returnsLogWriterLevelWarning() {
    assertThat(fromLevel(Level.WARN)).isEqualTo(LogWriterLevel.WARNING);
  }

  @Test
  public void toLogWriterLevel_LevelInfo_returnsLogWriterLevelInfo() {
    assertThat(fromLevel(Level.INFO)).isEqualTo(LogWriterLevel.INFO);
  }

  @Test
  public void toLogWriterLevel_LevelDebug_returnsLogWriterLevelFine() {
    assertThat(fromLevel(Level.DEBUG)).isEqualTo(LogWriterLevel.FINE);
  }

  @Test
  public void toLogWriterLevel_LevelTrace_returnsLogWriterLevelFinest() {
    assertThat(fromLevel(Level.TRACE)).isEqualTo(LogWriterLevel.FINEST);
  }

  @Test
  public void toLogWriterLevel_LevelOff_returnsLogWriterLevelNone() {
    assertThat(fromLevel(Level.OFF)).isEqualTo(LogWriterLevel.NONE);
  }

  @Test
  public void getLog4jLevel_nonLevel_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> toLevel(LogWriterLevel.find(123123123)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No LogWriterLevel found for intLevel 123123123");
  }

}
