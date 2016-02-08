/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.logging;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.i18n.StringId;

/**
 * Each logger has a level and it will only print messages whose
 * level is greater than or equal to the logger's level. The supported
 * logger level constants, in ascending order, are:
 * <ol>
 * <li> {@link #ALL_LEVEL}
 * <li> {@link #FINEST_LEVEL}
 * <li> {@link #FINER_LEVEL}
 * <li> {@link #FINE_LEVEL}
 * <li> {@link #CONFIG_LEVEL}
 * <li> {@link #INFO_LEVEL}
 * <li> {@link #WARNING_LEVEL}
 * <li> {@link #ERROR_LEVEL}
 * <li> {@link #SEVERE_LEVEL}
 * <li> {@link #NONE_LEVEL}
 * </ol>
 */
public interface InternalLogWriter extends LogWriter, LogWriterI18n {

  /**
   * If the writer's level is <code>ALL_LEVEL</code> then all messages
   * will be logged.
   */
  public final static int ALL_LEVEL = Integer.MIN_VALUE;
  /**
   * If the writer's level is <code>FINEST_LEVEL</code> then
   * finest, finer, fine, config, info, warning, error, and severe messages will be logged.
   */
  public final static int FINEST_LEVEL = 300;
  /**
   * If the writer's level is <code>FINER_LEVEL</code> then
   * finer, fine, config, info, warning, error, and severe messages will be logged.
   */
  public final static int FINER_LEVEL = 400;
  /**
   * If the writer's level is <code>FINE_LEVEL</code> then
   * fine, config, info, warning, error, and severe messages will be logged.
   */
  public final static int FINE_LEVEL = 500;
  /**
   * If the writer's level is <code>CONFIG_LEVEL</code> then
   * config, info, warning, error, and severe messages will be logged.
   */
  public final static int CONFIG_LEVEL = 700;
  /**
   * If the writer's level is <code>INFO_LEVEL</code> then
   * info, warning, error, and severe messages will be logged.
   */
  public final static int INFO_LEVEL = 800;
  /**
   * If the writer's level is <code>WARNING_LEVEL</code> then
   * warning, error, and severe messages will be logged.
   */
  public final static int WARNING_LEVEL = 900;
  /**
   * If the writer's level is <code>SEVERE_LEVEL</code> then
   * only severe messages will be logged.
   */
  public final static int SEVERE_LEVEL = 1000;
  /**
   * If the writer's level is <code>ERROR_LEVEL</code> then
   * error and severe messages will be logged.
   */
  public final static int ERROR_LEVEL = (WARNING_LEVEL + SEVERE_LEVEL) / 2;
  /**
   * If the writer's level is <code>NONE_LEVEL</code> then no messages
   * will be logged.
   */
  public final static int NONE_LEVEL = Integer.MAX_VALUE;

  public static final String[] levelNames = new String[] {
    "all", "finest", "finer", "fine", "config", "info", "warning", "error", "severe", "none"
   };

   public static final int[] allLevels = new int[] {
     ALL_LEVEL, FINEST_LEVEL, FINER_LEVEL, FINE_LEVEL, CONFIG_LEVEL, INFO_LEVEL, WARNING_LEVEL, ERROR_LEVEL, SEVERE_LEVEL, NONE_LEVEL
   };

   public int getLogWriterLevel();
   
   public void setLogWriterLevel(int LogWriterLevel);
   
   public boolean isSecure();
   
   public String getConnectionName();
   
   /**
    * Logs a message and an exception of the given level.
    * 
    * @param msgLevel
    *                the level code for the message to log
    * @param msg
    *                the actual message to log
    * @param exception
    *                the actual Exception to log
    */
   public void put(int msgLevel, String msg, Throwable exception);
   
   /**
    * Logs a message and an exception of the given level.
    * 
    * @param msgLevel
    *                the level code for the message to log
    * @param msgId
    *                A locale agnostic form of the message
    * @param params
    *                the Object arguments to plug into the message               
    * @param exception
    *                the actual Exception to log
    */
   public void put(int msgLevel, StringId msgId, Object[] params, Throwable exception);
}
