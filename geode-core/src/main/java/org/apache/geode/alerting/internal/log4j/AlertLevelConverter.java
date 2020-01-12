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

import static org.apache.geode.alerting.internal.spi.AlertLevel.ERROR;
import static org.apache.geode.alerting.internal.spi.AlertLevel.NONE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.SEVERE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.WARNING;

import org.apache.logging.log4j.Level;

import org.apache.geode.alerting.internal.spi.AlertLevel;

/**
 * Converts between {@link AlertLevel}s and Log4J2 {@code Level}s.
 *
 * <p>
 * Implementation note: switch and if-else structures perform better than using a HashMap for this
 * which matters the most if used for every log statement.
 */
public class AlertLevelConverter {

  /**
   * True if the Log4J2 {@code Level} converts to an {@link AlertLevel}.
   */
  public static boolean hasAlertLevel(final Level level) {
    if (level == Level.FATAL) {
      return true;
    } else if (level == Level.ERROR) {
      return true;
    } else if (level == Level.WARN) {
      return true;
    } else {
      return level == Level.OFF;
    }
  }

  /**
   * Converts from an {@link AlertLevel} to a Log4J2 {@code Level}.
   *
   * @throws IllegalArgumentException if there is no matching Log4J2 Level
   */
  public static Level toLevel(final AlertLevel alert) {
    switch (alert) {
      case SEVERE:
        return Level.FATAL;
      case ERROR:
        return Level.ERROR;
      case WARNING:
        return Level.WARN;
      case NONE:
        return Level.OFF;
    }

    throw new IllegalArgumentException("No matching Log4J2 Level for " + alert + ".");
  }

  /**
   * Converts from a Log4J2 {@code Level} to an {@link AlertLevel}.
   *
   * @throws IllegalArgumentException if there is no matching Alert
   */
  public static AlertLevel fromLevel(final Level level) {
    if (level == Level.FATAL) {
      return SEVERE;
    } else if (level == Level.ERROR) {
      return ERROR;
    } else if (level == Level.WARN) {
      return WARNING;
    } else if (level == Level.OFF) {
      return NONE;
    }

    throw new IllegalArgumentException("No matching AlertLevel for Log4J2 Level " + level + ".");
  }
}
