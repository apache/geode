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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;

import org.apache.geode.internal.logging.InternalLogWriter;

/**
 * This class provides utility methods to hold all valid log4j levels and legacy geode log levels
 * and the mapping between the two level hierarchy.
 */
public class LogLevel {

  private static Map<String, Level> LEVELS = new HashMap<>();
  private static Map<String, Integer> S2I = new HashMap<>();
  private static Map<Integer, String> I2S = new HashMap<>();

  static {
    // logwriter int level to log4j level string
    I2S.put(InternalLogWriter.NONE_LEVEL, "OFF");
    I2S.put(InternalLogWriter.SEVERE_LEVEL, "FATAL");
    I2S.put(InternalLogWriter.ERROR_LEVEL, "ERROR");
    I2S.put(InternalLogWriter.WARNING_LEVEL, "WARN");
    I2S.put(InternalLogWriter.INFO_LEVEL, "INFO");
    I2S.put(InternalLogWriter.CONFIG_LEVEL, "INFO");
    I2S.put(InternalLogWriter.FINE_LEVEL, "DEBUG");
    I2S.put(InternalLogWriter.FINER_LEVEL, "TRACE");
    I2S.put(InternalLogWriter.FINEST_LEVEL, "TRACE");
    I2S.put(InternalLogWriter.ALL_LEVEL, "ALL");

    // logwriter strings to integer
    S2I.put("NONE", InternalLogWriter.NONE_LEVEL);
    S2I.put("SEVERE", InternalLogWriter.SEVERE_LEVEL);
    S2I.put("ERROR", InternalLogWriter.ERROR_LEVEL);
    S2I.put("WARNING", InternalLogWriter.WARNING_LEVEL);
    S2I.put("INFO", InternalLogWriter.INFO_LEVEL);
    S2I.put("CONFIG", InternalLogWriter.CONFIG_LEVEL);
    S2I.put("FINE", InternalLogWriter.FINE_LEVEL);
    S2I.put("FINER", InternalLogWriter.FINER_LEVEL);
    S2I.put("FINEST", InternalLogWriter.FINEST_LEVEL);
    S2I.put("ALL", InternalLogWriter.ALL_LEVEL);

    // additional log4j strings to integer
    S2I.put("OFF", InternalLogWriter.NONE_LEVEL);
    S2I.put("FATAL", InternalLogWriter.SEVERE_LEVEL);
    S2I.put("WARN", InternalLogWriter.WARNING_LEVEL);
    S2I.put("DEBUG", InternalLogWriter.FINE_LEVEL);
    S2I.put("TRACE", InternalLogWriter.FINEST_LEVEL);

    // put all the log4j levels in the map first
    Arrays.stream(Level.values()).forEach(level -> {
      LEVELS.put(level.name(), level);
    });

    // map all the other logwriter level to log4j levels
    LEVELS.put("SEVERE", getLog4jLevel(InternalLogWriter.SEVERE_LEVEL));
    LEVELS.put("WARNING", getLog4jLevel(InternalLogWriter.WARNING_LEVEL));
    LEVELS.put("CONFIG", getLog4jLevel(InternalLogWriter.CONFIG_LEVEL));
    LEVELS.put("FINE", getLog4jLevel(InternalLogWriter.FINE_LEVEL));
    LEVELS.put("FINER", getLog4jLevel(InternalLogWriter.FINER_LEVEL));
    LEVELS.put("FINEST", getLog4jLevel(InternalLogWriter.FINEST_LEVEL));
    LEVELS.put("NONE", getLog4jLevel(InternalLogWriter.NONE_LEVEL));
  }

  /**
   * resolve the log4j level from any log statement in the log file.
   *
   * @param level either legacy level string or log4j level string
   * @return log4j level. Level.OFF is invalid string
   */
  public static Level resolveLevel(final String level) {
    Level log4jLevel = LEVELS.get(level.toUpperCase());
    // make sure any unrecognizable log level is assigned a most specific level
    return log4jLevel == null ? Level.OFF : log4jLevel;
  }

  /**
   * get Log4j Level from either legacy level string or log4j level string
   *
   * @param level either legacy level string or log4j level string
   * @return log4j level. null if invalid level string
   */
  public static Level getLevel(String level) {
    return LEVELS.get(level.toUpperCase());
  }

  /**
   * convert log4j level to logwriter code
   *
   * @param log4jLevel log4j level object
   * @return legacy logwriter code
   */
  public static int getLogWriterLevel(final Level log4jLevel) {
    Integer result = S2I.get(log4jLevel.name());

    if (result == null)
      throw new IllegalArgumentException("Unknown Log4J level [" + log4jLevel + "].");

    return result;
  }

  /**
   * convert legacy logwriter code to log4j level
   *
   * @param logWriterLevel logwriter code
   * @return log4j level
   */
  public static Level getLog4jLevel(final int logWriterLevel) {
    String log4jLevel = I2S.get(logWriterLevel);
    if (log4jLevel == null)
      throw new IllegalArgumentException("Unknown LogWriter level [" + logWriterLevel + "].");
    return Level.getLevel(log4jLevel);
  }

  /**
   * convert a string to logwriter code, either log4j level or logwriter string, or a level-xxx
   *
   * @param levelName a string of level name
   * @return logwriter code
   */
  public static int getLogWriterLevel(final String levelName) {
    if (levelName == null) {
      throw new IllegalArgumentException("LevelName cannot be null");
    }

    Integer level = S2I.get(levelName.toUpperCase());
    if (level != null)
      return level;

    if (levelName.startsWith("level-")) {
      String levelValue = levelName.substring("level-".length());
      return Integer.parseInt(levelValue);
    }

    String values =
        Arrays.stream(Level.values()).sorted().map(Level::name).collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Unknown log-level \"" + levelName + "\". Valid levels are: " + values + ".");
  }

  /**
   * convert a legacy logwriter code to log4j level string
   *
   * @param logWriterLevel integer code
   * @return log4j level string
   */
  public static String getLog4jLevelAsString(final int logWriterLevel) {
    return getLog4jLevel(logWriterLevel).name().toLowerCase();
  }
}
