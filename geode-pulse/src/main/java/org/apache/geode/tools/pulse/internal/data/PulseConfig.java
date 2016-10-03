/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import java.util.logging.Level;

/**
 * Class PulseConfig
 * 
 * PulseConfig is used for configuring Pulse application.
 * 
 * @since GemFire 7.0.1
 * 
 */
public class PulseConfig {

  // Log file name
  private String LogFileName;

  // Log file location
  private String LogFileLocation;

  // Log file size in MBs
  private int logFileSize;

  // Number of cyclic log files
  private int logFileCount;

  // Log messages date pattern
  private String logDatePattern;

  // Log level
  private Level logLevel;

  // Flag for appending log messages
  private Boolean logAppend;

  // Query history log file
  private String queryHistoryFileName;

  public PulseConfig() {
    this.setLogFileName(PulseConstants.PULSE_LOG_FILE_NAME);
    this.LogFileLocation = PulseConstants.PULSE_LOG_FILE_LOCATION;
    this.logFileSize = PulseConstants.PULSE_LOG_FILE_SIZE;
    this.logFileCount = PulseConstants.PULSE_LOG_FILE_COUNT;
    this.logDatePattern = PulseConstants.PULSE_LOG_MESSAGE_DATE_PATTERN;
    this.logLevel = PulseConstants.PULSE_LOG_LEVEL;
    this.logAppend = PulseConstants.PULSE_LOG_APPEND;
    this.queryHistoryFileName = PulseConstants.PULSE_QUERY_HISTORY_FILE_LOCATION
      + System.getProperty("file.separator")
      + PulseConstants.PULSE_QUERY_HISTORY_FILE_NAME;

  }

  public String getLogFileName() {
    return LogFileName;
  }

  public void setLogFileName(String logFileName) {
    this.LogFileName = logFileName + "_%g.log";
  }

  public String getLogFileLocation() {
    return LogFileLocation;
  }

  public void setLogFileLocation(String logFileLocation) {
    this.LogFileLocation = logFileLocation;
  }

  public String getLogFileFullName() {
    return this.LogFileLocation + "/" + this.LogFileName;
  }

  public int getLogFileSize() {
    return logFileSize;
  }

  public void setLogFileSize(int logFileSize) {
    this.logFileSize = logFileSize;
  }

  public int getLogFileCount() {
    return logFileCount;
  }

  public void setLogFileCount(int logFileCount) {
    this.logFileCount = logFileCount;
  }

  public String getLogDatePattern() {
    return logDatePattern;
  }

  public void setLogDatePattern(String logDatePattern) {
    this.logDatePattern = logDatePattern;
  }

  public Level getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(Level logLevel) {
    this.logLevel = logLevel;
  }

  public Boolean getLogAppend() {
    return logAppend;
  }

  public void setLogAppend(Boolean logAppend) {
    this.logAppend = logAppend;
  }

  public String getQueryHistoryFileName() {
    return queryHistoryFileName;
  }

  public void setQueryHistoryFileName(String queryHistoryFileName) {
    this.queryHistoryFileName = queryHistoryFileName;
  }
}
