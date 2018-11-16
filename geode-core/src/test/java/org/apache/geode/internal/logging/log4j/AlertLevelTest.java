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

import static org.apache.geode.internal.logging.log4j.AlertLevel.alertLevelToLogLevel;
import static org.apache.geode.internal.logging.log4j.AlertLevel.logLevelToAlertLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.admin.Alert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link AlertLevel}.
 */
@Category(LoggingTest.class)
public class AlertLevelTest {

  @Test
  public void alertLevelToLogLevel_AlertSevere_returnsLevelFatal() {
    assertThat(alertLevelToLogLevel(Alert.SEVERE)).isEqualTo(Level.FATAL.intLevel());
  }

  @Test
  public void alertLevelToLogLevel_AlertError_returnsLevelFatal() {
    assertThat(alertLevelToLogLevel(Alert.ERROR)).isEqualTo(Level.ERROR.intLevel());
  }

  @Test
  public void alertLevelToLogLevel_AlertWarning_returnsLevelFatal() {
    assertThat(alertLevelToLogLevel(Alert.WARNING)).isEqualTo(Level.WARN.intLevel());
  }

  @Test
  public void alertLevelToLogLevel_AlertOff_returnsLevelFatal() {
    assertThat(alertLevelToLogLevel(Alert.OFF)).isEqualTo(Level.OFF.intLevel());
  }

  @Test
  public void alertLevelToLogLevel_AlertAll_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> alertLevelToLogLevel(Alert.ALL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown Alert level [" + Alert.ALL + "].");
  }

  @Test
  public void alertLevelToLogLevel_AlertFinest_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> alertLevelToLogLevel(Alert.FINEST))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown Alert level [" + Alert.FINEST + "].");
  }

  @Test
  public void alertLevelToLogLevel_AlertFine_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> alertLevelToLogLevel(Alert.FINE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown Alert level [" + Alert.FINE + "].");
  }

  @Test
  public void alertLevelToLogLevel_AlertConfig_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> alertLevelToLogLevel(Alert.CONFIG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown Alert level [" + Alert.CONFIG + "].");
  }

  @Test
  public void alertLevelToLogLevel_AlertInfo_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> alertLevelToLogLevel(Alert.INFO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown Alert level [" + Alert.INFO + "].");
  }

  @Test
  public void logLevelToAlertLevel_LevelFatal_returnsAlertSevere() {
    assertThat(logLevelToAlertLevel(Level.FATAL)).isEqualTo(Alert.SEVERE);
  }

  @Test
  public void logLevelToAlertLevel_LevelError_returnsAlertError() {
    assertThat(logLevelToAlertLevel(Level.ERROR)).isEqualTo(Alert.ERROR);
  }

  @Test
  public void logLevelToAlertLevel_LevelWarn_returnsAlertWarning() {
    assertThat(logLevelToAlertLevel(Level.WARN)).isEqualTo(Alert.WARNING);
  }

  @Test
  public void logLevelToAlertLevel_LevelOff_returnsAlertOff() {
    assertThat(logLevelToAlertLevel(Level.OFF)).isEqualTo(Alert.OFF);
  }

  @Test
  public void logLevelToAlertLevel_LevelAll_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> logLevelToAlertLevel(Level.ALL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown LOG4J2 Level [" + Level.ALL + "].");
  }

  @Test
  public void logLevelToAlertLevel_LevelTrace_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> logLevelToAlertLevel(Level.TRACE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown LOG4J2 Level [" + Level.TRACE + "].");
  }

  @Test
  public void logLevelToAlertLevel_LevelDebug_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> logLevelToAlertLevel(Level.DEBUG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown LOG4J2 Level [" + Level.DEBUG + "].");
  }

  @Test
  public void logLevelToAlertLevel_LevelInfo_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> logLevelToAlertLevel(Level.INFO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown LOG4J2 Level [" + Level.INFO + "].");
  }
}
