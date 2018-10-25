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
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.LogWriterLevel;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category(LoggingTest.class)
public class LogLevelTest {

  @Test
  public void getLevel_log4J2LevelName_returnsLevel() {
    assertThat(LogLevel.getLevel(Level.OFF.name())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(Level.FATAL.name())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(Level.ERROR.name())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(Level.WARN.name())).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(Level.INFO.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(Level.DEBUG.name())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(Level.TRACE.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(Level.ALL.name())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_log4J2LevelName_toLowerCase_returnsLevel() {
    assertThat(LogLevel.getLevel(Level.OFF.name().toLowerCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(Level.FATAL.name().toLowerCase())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(Level.ERROR.name().toLowerCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(Level.WARN.name().toLowerCase())).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(Level.INFO.name().toLowerCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(Level.DEBUG.name().toLowerCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(Level.TRACE.name().toLowerCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(Level.ALL.name().toLowerCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_log4J2LevelName_toUpperCase_returnsLevel() {
    assertThat(LogLevel.getLevel(Level.OFF.name().toUpperCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(Level.FATAL.name().toUpperCase())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(Level.ERROR.name().toUpperCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(Level.WARN.name().toUpperCase())).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(Level.INFO.name().toUpperCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(Level.DEBUG.name().toUpperCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(Level.TRACE.name().toUpperCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(Level.ALL.name().toUpperCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_logWriterLevelName_returnsLevel() {
    assertThat(LogLevel.getLevel(LogWriterLevel.NONE.name())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(LogWriterLevel.SEVERE.name())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(LogWriterLevel.ERROR.name())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(LogWriterLevel.WARNING.name())).isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(LogWriterLevel.INFO.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.CONFIG.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINE.name())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINER.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINEST.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.ALL.name())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_logWriterLevelName_toLowerCase_returnsLevel() {
    assertThat(LogLevel.getLevel(LogWriterLevel.NONE.name().toLowerCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(LogWriterLevel.SEVERE.name().toLowerCase()))
        .isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(LogWriterLevel.ERROR.name().toLowerCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(LogWriterLevel.WARNING.name().toLowerCase()))
        .isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(LogWriterLevel.INFO.name().toLowerCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.CONFIG.name().toLowerCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINE.name().toLowerCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINER.name().toLowerCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINEST.name().toLowerCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.ALL.name().toLowerCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_logWriterLevelName_toUpperCase_returnsLevel() {
    assertThat(LogLevel.getLevel(LogWriterLevel.NONE.name().toUpperCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.getLevel(LogWriterLevel.SEVERE.name().toUpperCase()))
        .isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLevel(LogWriterLevel.ERROR.name().toUpperCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLevel(LogWriterLevel.WARNING.name().toUpperCase()))
        .isEqualTo(Level.WARN);
    assertThat(LogLevel.getLevel(LogWriterLevel.INFO.name().toUpperCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.CONFIG.name().toUpperCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINE.name().toUpperCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINER.name().toUpperCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.FINEST.name().toUpperCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLevel(LogWriterLevel.ALL.name().toUpperCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLevel_nonLevel_returnsNull() {
    assertThat(LogLevel.getLevel("notrecognizable")).isNull();
  }

  @Test
  public void resolveLevel_log4J2LevelName_returnsLevel() {
    assertThat(LogLevel.resolveLevel(Level.OFF.name())).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(Level.FATAL.name())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(Level.ERROR.name())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(Level.WARN.name())).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(Level.INFO.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(Level.DEBUG.name())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(Level.TRACE.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(Level.ALL.name())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_log4J2LevelName_toLowerCase_returnsLevel() {
    assertThat(LogLevel.resolveLevel(Level.OFF.name().toLowerCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(Level.FATAL.name().toLowerCase())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(Level.ERROR.name().toLowerCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(Level.WARN.name().toLowerCase())).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(Level.INFO.name().toLowerCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(Level.DEBUG.name().toLowerCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(Level.TRACE.name().toLowerCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(Level.ALL.name().toLowerCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_log4J2LevelName_toUpperCase_returnsLevel() {
    assertThat(LogLevel.resolveLevel(Level.OFF.name().toUpperCase())).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(Level.FATAL.name().toUpperCase())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(Level.ERROR.name().toUpperCase())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(Level.WARN.name().toUpperCase())).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(Level.INFO.name().toUpperCase())).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(Level.DEBUG.name().toUpperCase())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(Level.TRACE.name().toUpperCase())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(Level.ALL.name().toUpperCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_logWriterLevel_returnsLevel() {
    assertThat(LogLevel.resolveLevel(LogWriterLevel.NONE.name())).isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.SEVERE.name())).isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ERROR.name())).isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.WARNING.name())).isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.INFO.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.CONFIG.name())).isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINE.name())).isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINER.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINEST.name())).isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ALL.name())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_logWriterLevel_toLowerCase_returnsLevel() {
    assertThat(LogLevel.resolveLevel(LogWriterLevel.NONE.name().toLowerCase()))
        .isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.SEVERE.name().toLowerCase()))
        .isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ERROR.name().toLowerCase()))
        .isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.WARNING.name().toLowerCase()))
        .isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.INFO.name().toLowerCase()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.CONFIG.name().toLowerCase()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINE.name().toLowerCase()))
        .isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINER.name().toLowerCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINEST.name().toLowerCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ALL.name().toLowerCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_logWriterLevel_toUpperCase_returnsLevel() {
    assertThat(LogLevel.resolveLevel(LogWriterLevel.NONE.name().toUpperCase()))
        .isEqualTo(Level.OFF);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.SEVERE.name().toUpperCase()))
        .isEqualTo(Level.FATAL);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ERROR.name().toUpperCase()))
        .isEqualTo(Level.ERROR);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.WARNING.name().toUpperCase()))
        .isEqualTo(Level.WARN);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.INFO.name().toUpperCase()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.CONFIG.name().toUpperCase()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINE.name().toUpperCase()))
        .isEqualTo(Level.DEBUG);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINER.name().toUpperCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.FINEST.name().toUpperCase()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.resolveLevel(LogWriterLevel.ALL.name().toUpperCase())).isEqualTo(Level.ALL);
  }

  @Test
  public void resolveLevel_nonLevel_returnsOff() {
    assertThat(LogLevel.resolveLevel("notrecognizable")).isEqualTo(Level.OFF);
  }

  @Test
  public void getLog4jLevel_logWriterLevel_returnsLevel() {
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.NONE.getLogWriterLevel()))
        .isEqualTo(Level.OFF);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.SEVERE.getLogWriterLevel()))
        .isEqualTo(Level.FATAL);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.ERROR.getLogWriterLevel()))
        .isEqualTo(Level.ERROR);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.WARNING.getLogWriterLevel()))
        .isEqualTo(Level.WARN);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.INFO.getLogWriterLevel()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.CONFIG.getLogWriterLevel()))
        .isEqualTo(Level.INFO);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.FINE.getLogWriterLevel()))
        .isEqualTo(Level.DEBUG);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.FINER.getLogWriterLevel()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.FINEST.getLogWriterLevel()))
        .isEqualTo(Level.TRACE);
    assertThat(LogLevel.getLog4jLevel(LogWriterLevel.ALL.getLogWriterLevel())).isEqualTo(Level.ALL);
  }

  @Test
  public void getLog4jLevel_nonLevel_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> LogLevel.getLog4jLevel(123123123))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown LogWriter level");
  }

  @Test
  public void getLogWriterLevel_log4j2Level_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(Level.OFF))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.FATAL))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ERROR))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.WARN))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.INFO))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.DEBUG))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.TRACE))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ALL))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_log4j2LevelName_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(Level.OFF.name()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.FATAL.name()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ERROR.name()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.WARN.name()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.INFO.name()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.DEBUG.name()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.TRACE.name()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ALL.name()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_log4j2LevelName_toLowerCase_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(Level.OFF.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.FATAL.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ERROR.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.WARN.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.INFO.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.DEBUG.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.TRACE.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ALL.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_log4j2LevelName_toUpperCase_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(Level.OFF.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.FATAL.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ERROR.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.WARN.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.INFO.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.DEBUG.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.TRACE.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(Level.ALL.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_logWriterLevelName_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.NONE.name()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.SEVERE.name()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ERROR.name()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.WARNING.name()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.CONFIG.name()))
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.INFO.name()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINE.name()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINER.name()))
        .isEqualTo(LogWriterLevel.FINER.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINEST.name()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ALL.name()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_logWriterLevelName_toLowerCase_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.NONE.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.SEVERE.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ERROR.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.WARNING.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.INFO.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.CONFIG.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINE.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINER.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.FINER.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINEST.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ALL.name().toLowerCase()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_logWriterLevelName_toUpperCase_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.NONE.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.NONE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.SEVERE.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.SEVERE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ERROR.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.WARNING.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.WARNING.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.INFO.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.CONFIG.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINE.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINER.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.FINER.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.FINEST.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.FINEST.getLogWriterLevel());
    assertThat(LogLevel.getLogWriterLevel(LogWriterLevel.ALL.name().toUpperCase()))
        .isEqualTo(LogWriterLevel.ALL.getLogWriterLevel());
  }

  @Test
  public void getLogWriterLevel_levelWithNumber_returnsLogWriterLevelValue() {
    assertThat(LogLevel.getLogWriterLevel("level-" + Integer.MIN_VALUE))
        .isEqualTo(Integer.MIN_VALUE);
    assertThat(LogLevel.getLogWriterLevel("level-0")).isEqualTo(0);
    assertThat(LogLevel.getLogWriterLevel("level-1")).isEqualTo(1);
    assertThat(LogLevel.getLogWriterLevel("level-1234")).isEqualTo(1234);
    assertThat(LogLevel.getLogWriterLevel("level-2345")).isEqualTo(2345);
    assertThat(LogLevel.getLogWriterLevel("level-" + Integer.MAX_VALUE))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void getLogWriterLevel_null_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> LogLevel.getLogWriterLevel((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("LevelName cannot be null");
  }

  @Test
  public void getLogWriterLevel_test_returns() {
    assertThatThrownBy(() -> LogLevel.getLogWriterLevel("test"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Unknown log-level \"test\". Valid levels are: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL.");
  }
}
