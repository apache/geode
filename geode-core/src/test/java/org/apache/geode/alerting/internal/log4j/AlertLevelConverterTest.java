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
package org.apache.geode.alerting.internal.log4j;

import static org.apache.geode.alerting.internal.log4j.AlertLevelConverter.fromLevel;
import static org.apache.geode.alerting.internal.log4j.AlertLevelConverter.hasAlertLevel;
import static org.apache.geode.alerting.internal.log4j.AlertLevelConverter.toLevel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link AlertLevelConverter}.
 */
@Category({AlertingTest.class, LoggingTest.class})
public class AlertLevelConverterTest {

  @Test
  public void toLevel_AlertSevere_returnsLevelFatal() {
    assertThat(toLevel(AlertLevel.SEVERE)).isEqualTo(Level.FATAL);
  }

  @Test
  public void toLevel_AlertError_returnsLevelFatal() {
    assertThat(toLevel(AlertLevel.ERROR)).isEqualTo(Level.ERROR);
  }

  @Test
  public void toLevel_AlertWarning_returnsLevelFatal() {
    assertThat(toLevel(AlertLevel.WARNING)).isEqualTo(Level.WARN);
  }

  @Test
  public void toLevel_AlertOff_returnsLevelFatal() {
    assertThat(toLevel(AlertLevel.NONE)).isEqualTo(Level.OFF);
  }

  @Test
  public void toAlertLevel_LevelFatal_returnsAlertSevere() {
    assertThat(fromLevel(Level.FATAL)).isEqualTo(AlertLevel.SEVERE);
  }

  @Test
  public void toAlertLevel_LevelError_returnsAlertError() {
    assertThat(fromLevel(Level.ERROR)).isEqualTo(AlertLevel.ERROR);
  }

  @Test
  public void toAlertLevel_LevelWarn_returnsAlertWarning() {
    assertThat(fromLevel(Level.WARN)).isEqualTo(AlertLevel.WARNING);
  }

  @Test
  public void toAlertLevel_LevelOff_returnsAlertNone() {
    assertThat(fromLevel(Level.OFF)).isEqualTo(AlertLevel.NONE);
  }

  @Test
  public void toAlertLevel_LevelAll_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> fromLevel(Level.ALL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No matching AlertLevel for Log4J2 Level " + Level.ALL + ".");
  }

  @Test
  public void toAlertLevel_LevelTrace_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> fromLevel(Level.TRACE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No matching AlertLevel for Log4J2 Level " + Level.TRACE + ".");
  }

  @Test
  public void toAlertLevel_LevelDebug_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> fromLevel(Level.DEBUG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No matching AlertLevel for Log4J2 Level " + Level.DEBUG + ".");
  }

  @Test
  public void toAlertLevel_LevelInfo_throwsIllegalArgumentException() {
    assertThatThrownBy(() -> fromLevel(Level.INFO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No matching AlertLevel for Log4J2 Level " + Level.INFO + ".");
  }

  @Test
  public void hasAlertLevel_LevelFatal_returnsTrue() {
    assertThat(hasAlertLevel(Level.FATAL)).isTrue();
  }

  @Test
  public void hasAlertLevel_LevelError_returnsTrue() {
    assertThat(hasAlertLevel(Level.ERROR)).isTrue();
  }

  @Test
  public void hasAlertLevel_LevelWarn_returnsTrue() {
    assertThat(hasAlertLevel(Level.WARN)).isTrue();
  }

  @Test
  public void hasAlertLevel_LevelOff_returnsTrue() {
    assertThat(hasAlertLevel(Level.OFF)).isTrue();
  }

  @Test
  public void hasAlertLevel_LevelAll_returnsFalse() {
    assertThat(hasAlertLevel(Level.ALL)).isFalse();
  }

  @Test
  public void hasAlertLevel_LevelTrace_returnsFalse() {
    assertThat(hasAlertLevel(Level.TRACE)).isFalse();
  }

  @Test
  public void hasAlertLevel_LevelDebug_returnsFalse() {
    assertThat(hasAlertLevel(Level.DEBUG)).isFalse();
  }

  @Test
  public void hasAlertLevel_LevelInfo_returnsFalse() {
    assertThat(hasAlertLevel(Level.INFO)).isFalse();
  }
}
