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
package org.apache.geode.logging.internal.spi;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.InternalLogWriter;

/**
 * Levels used for identifying the severity of a {@link LogWriter} event. From least specific to
 * most:
 * <ul>
 * <li>{@link #ALL} (least specific, all logging)</li>
 * <li>{@link #FINEST} (least specific, a lot of logging)</li>
 * <li>{@link #FINER}</li>
 * <li>{@link #FINE}</li>
 * <li>{@link #CONFIG}</li>
 * <li>{@link #INFO}</li>
 * <li>{@link #WARNING}</li>
 * <li>{@link #ERROR}</li>
 * <li>{@link #SEVERE} (most specific, a little logging)</li>
 * <li>{@link #NONE} (most specific, no logging)</li>
 * </ul>
 *
 * <p>
 * Default level in Geode is {@link #CONFIG}.
 */
public enum LogWriterLevel {

  ALL(InternalLogWriter.ALL_LEVEL),
  FINEST(InternalLogWriter.FINEST_LEVEL),
  FINER(InternalLogWriter.FINER_LEVEL),
  FINE(InternalLogWriter.FINE_LEVEL),
  CONFIG(InternalLogWriter.CONFIG_LEVEL), // default
  INFO(InternalLogWriter.INFO_LEVEL),
  WARNING(InternalLogWriter.WARNING_LEVEL),
  ERROR(InternalLogWriter.ERROR_LEVEL),
  SEVERE(InternalLogWriter.SEVERE_LEVEL),
  NONE(InternalLogWriter.NONE_LEVEL);

  public static LogWriterLevel find(final int intLevel) {
    for (LogWriterLevel logWriterLevel : values()) {
      if (logWriterLevel.intLevel == intLevel) {
        return logWriterLevel;
      }
    }
    throw new IllegalArgumentException("No LogWriterLevel found for intLevel " + intLevel);
  }

  private final int intLevel;

  LogWriterLevel(final int intLevel) {
    this.intLevel = intLevel;
  }

  public int intLevel() {
    return intLevel;
  }
}
