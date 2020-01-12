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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.logging.internal.spi.LogWriterLevel;

/**
 * Provides lookup of any string representation of a logging level to Log4J2 {@code Level} or
 * {@code LogWriterLevel} int value.
 */
public class LogLevel {

  @Immutable
  private static final Map<String, Level> ANY_NAME_TO_LEVEL;
  @Immutable
  private static final Map<String, LogWriterLevel> ANY_NAME_TO_LOGWRITERLEVEL;

  static {
    Map<String, Level> nameToLevel = new HashMap<>();
    Map<String, LogWriterLevel> nameToLogLevel = new HashMap<>();
    // LogWriterLevel name to LogWriterLevel
    nameToLogLevel.put(LogWriterLevel.NONE.name(), LogWriterLevel.NONE);
    nameToLogLevel.put(LogWriterLevel.SEVERE.name(), LogWriterLevel.SEVERE);
    nameToLogLevel.put(LogWriterLevel.ERROR.name(), LogWriterLevel.ERROR);
    nameToLogLevel.put(LogWriterLevel.WARNING.name(), LogWriterLevel.WARNING);
    nameToLogLevel.put(LogWriterLevel.INFO.name(), LogWriterLevel.INFO);
    nameToLogLevel.put(LogWriterLevel.CONFIG.name(), LogWriterLevel.CONFIG);
    nameToLogLevel.put(LogWriterLevel.FINE.name(), LogWriterLevel.FINE);
    nameToLogLevel.put(LogWriterLevel.FINER.name(), LogWriterLevel.FINER);
    nameToLogLevel.put(LogWriterLevel.FINEST.name(), LogWriterLevel.FINEST);
    nameToLogLevel.put(LogWriterLevel.ALL.name(), LogWriterLevel.ALL);

    // additional Log4J2 names to LogWriterLevel
    nameToLogLevel.put(Level.OFF.name(), LogWriterLevel.NONE);
    nameToLogLevel.put(Level.FATAL.name(), LogWriterLevel.SEVERE);
    nameToLogLevel.put(Level.WARN.name(), LogWriterLevel.WARNING);
    nameToLogLevel.put(Level.DEBUG.name(), LogWriterLevel.FINE);
    nameToLogLevel.put(Level.TRACE.name(), LogWriterLevel.FINEST);

    // put all the log4j levels in the map first
    Arrays.stream(Level.values()).forEach(level -> {
      nameToLevel.put(level.name(), level);
    });

    // map all the other logwriter level to log4j levels
    nameToLevel.put(LogWriterLevel.SEVERE.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.SEVERE.intLevel())));
    nameToLevel.put(LogWriterLevel.WARNING.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.WARNING.intLevel())));
    nameToLevel.put(LogWriterLevel.CONFIG.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.CONFIG.intLevel())));
    nameToLevel.put(LogWriterLevel.FINE.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.FINE.intLevel())));
    nameToLevel.put(LogWriterLevel.FINER.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.FINER.intLevel())));
    nameToLevel.put(LogWriterLevel.FINEST.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.FINEST.intLevel())));
    nameToLevel.put(LogWriterLevel.NONE.name(),
        LogWriterLevelConverter.toLevel(LogWriterLevel.find(LogWriterLevel.NONE.intLevel())));

    ANY_NAME_TO_LOGWRITERLEVEL = Collections.unmodifiableMap(nameToLogLevel);
    ANY_NAME_TO_LEVEL = Collections.unmodifiableMap(nameToLevel);
  }

  /**
   * Convert any string representation of a logging level to a Log4J2 {@code Level}. Returns
   * {@code Level.OFF} if invalid.
   *
   * <p>
   * resolve the log4j level from any log statement in the log file.
   */
  public static Level resolveLevel(final String anyLevelName) {
    Level log4jLevel = ANY_NAME_TO_LEVEL.get(anyLevelName.toUpperCase());
    // make sure any unrecognizable log level is assigned a most specific level
    return log4jLevel == null ? Level.OFF : log4jLevel;
  }

  /**
   * Convert any string representation of a logging level to a Log4J2 {@code Level}. Returns null
   * if invalid.
   *
   * <p>
   * get Log4j Level from either legacy level string or log4j level string
   */
  public static Level getLevel(String anyLevelName) {
    return ANY_NAME_TO_LEVEL.get(anyLevelName.toUpperCase());
  }

  /**
   * Convert any string representation of a logging level to a {@code LogWriterLevel} int value.
   *
   * <p>
   * convert a string to logwriter code, either log4j level or logwriter string, or a level-xxx
   */
  public static int getLogWriterLevel(final String anyLevelName) {
    if (anyLevelName == null) {
      throw new IllegalArgumentException("LevelName cannot be null");
    }

    if (ANY_NAME_TO_LOGWRITERLEVEL.get(anyLevelName.toUpperCase()) != null) {
      return ANY_NAME_TO_LOGWRITERLEVEL.get(anyLevelName.toUpperCase()).intLevel();
    }

    if (anyLevelName.toLowerCase().startsWith("level-")) {
      String levelValue = anyLevelName.toLowerCase().substring("level-".length());
      return Integer.parseInt(levelValue);
    }

    String values =
        Arrays.stream(Level.values()).sorted().map(Level::name).collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Unknown log-level \"" + anyLevelName + "\". Valid levels are: " + values + ".");
  }
}
