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
package org.apache.geode.internal.logging;

import org.apache.geode.LogWriter;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.i18n.StringId;

/**
 * Each logger has a level and it will only print messages whose level is greater than or equal to
 * the logger's level. The supported logger level constants, in ascending order, are:
 * <ol>
 * <li>{@link #ALL_LEVEL}
 * <li>{@link #FINEST_LEVEL}
 * <li>{@link #FINER_LEVEL}
 * <li>{@link #FINE_LEVEL}
 * <li>{@link #CONFIG_LEVEL}
 * <li>{@link #INFO_LEVEL}
 * <li>{@link #WARNING_LEVEL}
 * <li>{@link #ERROR_LEVEL}
 * <li>{@link #SEVERE_LEVEL}
 * <li>{@link #NONE_LEVEL}
 * </ol>
 *
 * @deprecated Please use Log4J2 instead.
 */
@Deprecated
public interface InternalLogWriter extends LogWriter, LogWriterI18n {

  /**
   * If the writer's level is <code>ALL_LEVEL</code> then all messages will be logged.
   */
  int ALL_LEVEL = Integer.MIN_VALUE;
  /**
   * If the writer's level is <code>FINEST_LEVEL</code> then finest, finer, fine, config, info,
   * warning, error, and severe messages will be logged.
   */
  int FINEST_LEVEL = 300;
  /**
   * If the writer's level is <code>FINER_LEVEL</code> then finer, fine, config, info, warning,
   * error, and severe messages will be logged.
   */
  int FINER_LEVEL = 400;
  /**
   * If the writer's level is <code>FINE_LEVEL</code> then fine, config, info, warning, error, and
   * severe messages will be logged.
   */
  int FINE_LEVEL = 500;
  /**
   * If the writer's level is <code>CONFIG_LEVEL</code> then config, info, warning, error, and
   * severe messages will be logged.
   */
  int CONFIG_LEVEL = 700;
  /**
   * If the writer's level is <code>INFO_LEVEL</code> then info, warning, error, and severe messages
   * will be logged.
   */
  int INFO_LEVEL = 800;
  /**
   * If the writer's level is <code>WARNING_LEVEL</code> then warning, error, and severe messages
   * will be logged.
   */
  int WARNING_LEVEL = 900;
  /**
   * If the writer's level is <code>SEVERE_LEVEL</code> then only severe messages will be logged.
   */
  int SEVERE_LEVEL = 1000;
  /**
   * If the writer's level is <code>ERROR_LEVEL</code> then error and severe messages will be
   * logged.
   */
  int ERROR_LEVEL = (WARNING_LEVEL + SEVERE_LEVEL) / 2;
  /**
   * If the writer's level is <code>NONE_LEVEL</code> then no messages will be logged.
   */
  int NONE_LEVEL = Integer.MAX_VALUE;

  String[] levelNames = new String[] {"all", "finest", "finer", "fine", "config", "info", "warning",
      "error", "severe", "none"};

  int[] allLevels = new int[] {ALL_LEVEL, FINEST_LEVEL, FINER_LEVEL, FINE_LEVEL, CONFIG_LEVEL,
      INFO_LEVEL, WARNING_LEVEL, ERROR_LEVEL, SEVERE_LEVEL, NONE_LEVEL};

  int getLogWriterLevel();

  void setLogWriterLevel(int logWriterLevel);

  boolean isSecure();

  String getConnectionName();

  /**
   * Logs a message and an exception of the given level.
   *
   * @param messageLevel the level code for the message to log
   * @param message the actual message to log
   * @param throwable the actual Exception to log
   */
  void put(int messageLevel, String message, Throwable throwable);

  /**
   * Logs a message and an exception of the given level.
   *
   * @param messageLevel the level code for the message to log
   * @param messageId A locale agnostic form of the message
   * @param parameters the Object arguments to plug into the message
   * @param throwable the actual Exception to log
   */
  void put(int messageLevel, StringId messageId, Object[] parameters, Throwable throwable);
}
