/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.data;

import java.util.logging.Level;

/**
 * Class PulseConfig
 * 
 * PulseConfig is used for configuring Pulse application.
 * 
 * @author Sachin K
 * @since 7.0.1
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

  public PulseConfig() {
    this.setLogFileName(PulseConstants.PULSE_LOG_FILE_NAME);
    this.LogFileLocation = PulseConstants.PULSE_LOG_FILE_LOCATION;
    this.logFileSize = PulseConstants.PULSE_LOG_FILE_SIZE;
    this.logFileCount = PulseConstants.PULSE_LOG_FILE_COUNT;
    this.logDatePattern = PulseConstants.PULSE_LOG_MESSAGE_DATE_PATTERN;
    this.logLevel = PulseConstants.PULSE_LOG_LEVEL;
    this.logAppend = PulseConstants.PULSE_LOG_APPEND;
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

}
