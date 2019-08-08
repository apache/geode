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
package org.apache.geode.logging.internal.log4j;


import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ERROR;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.NONE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.SEVERE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;

import org.apache.logging.log4j.Level;

import org.apache.geode.logging.internal.spi.LogWriterLevel;

/**
 * Converts between {@link LogWriterLevel}s and Log4J2 {@code Level}s.
 *
 * <p>
 * Implementation note: switch and if-else structures perform better than using a HashMap for this
 * which matters the most if used for every log statement.
 */
public class LogWriterLevelConverter {

  /**
   * Converts from a {@link LogWriterLevel} to a Log4J2 {@code Level}.
   *
   * @throws IllegalArgumentException if there is no matching Log4J2 Level
   */
  public static Level toLevel(final LogWriterLevel logWriterLevel) {
    switch (logWriterLevel) {
      case ALL:
        return Level.ALL;
      case SEVERE:
        return Level.FATAL;
      case ERROR:
        return Level.ERROR;
      case WARNING:
        return Level.WARN;
      case INFO:
        return Level.INFO;
      case CONFIG:
        return Level.INFO;
      case FINE:
        return Level.DEBUG;
      case FINER:
        return Level.TRACE;
      case FINEST:
        return Level.TRACE;
      case NONE:
        return Level.OFF;
    }

    throw new IllegalArgumentException("No matching Log4J2 Level for " + logWriterLevel + ".");
  }

  /**
   * Converts from a Log4J2 {@code Level} to a {@link LogWriterLevel}.
   *
   * @throws IllegalArgumentException if there is no matching Alert
   */
  public static LogWriterLevel fromLevel(final Level level) {
    if (level == Level.ALL) {
      return ALL;
    } else if (level == Level.FATAL) {
      return SEVERE;
    } else if (level == Level.ERROR) {
      return ERROR;
    } else if (level == Level.WARN) {
      return WARNING;
    } else if (level == Level.INFO) {
      return INFO;
    } else if (level == Level.DEBUG) {
      return FINE;
    } else if (level == Level.TRACE) {
      return FINEST;
    } else if (level == Level.OFF) {
      return NONE;
    }

    throw new IllegalArgumentException("No matching AlertLevel for Log4J2 Level " + level + ".");
  }
}
