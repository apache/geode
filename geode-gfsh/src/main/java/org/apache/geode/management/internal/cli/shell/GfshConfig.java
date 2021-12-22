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
package org.apache.geode.management.internal.cli.shell;

import java.io.File;
import java.util.logging.Level;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.util.IOUtils;

/**
 *
 * @since GemFire 7.0
 */
// According to 8.0 discussions, gfsh should have as less confing as possible
// hence Persisting GfshConfig is not done
public class GfshConfig {
  public static final String INIT_FILE_PROPERTY = "gfsh.init-file";
  private static final String LOG_DIR_PROPERTY = "gfsh.log-dir";
  private static final String LOG_LEVEL_PROPERTY = "gfsh.log-level";
  private static final String LOG_FILE_SIZE_LIMIT_PROPERTY = "gfsh.log-file-size-limit";
  private static final String LOG_DISK_SPACE_LIMIT_PROPERTY = "gfsh.log-disk-space-limit";

  private static final File HISTORY_FILE = new File(getHomeGemFireDirectory(), ".gfsh.history");

  // History file size
  private static final int MAX_HISTORY_SIZE = 500;

  public static final String DEFAULT_INIT_FILE_NAME = ".gfsh2rc";
  @Immutable
  private static final Level DEFAULT_LOGLEVEL = Level.OFF;
  private static final int DEFAULT_LOGFILE_SIZE_LIMIT = 1024 * 1024 * 10;
  private static final int DEFAULT_LOGFILE_DISK_USAGE = 1024 * 1024 * 10;

  private static final String DEFAULT_PROMPT = "{0}gfsh{1}>";

  private final String historyFileName;
  private final String initFileName;
  private final String defaultPrompt;
  private final int historySize;
  private final String logDir;
  private final Level logLevel;
  private final int logFileSizeLimit;
  private final int logFileDiskLimit;

  public GfshConfig() {
    this(HISTORY_FILE.getAbsolutePath(), DEFAULT_PROMPT, MAX_HISTORY_SIZE, null, null, null, null,
        null);
  }

  public boolean deleteHistoryFile() {
    if (historyFileName == null) {
      return true;
    }

    File file = new File(historyFileName);
    if (!file.exists()) {
      return true;
    }

    return file.delete();
  }

  public GfshConfig(String historyFileName, String defaultPrompt, int historySize, String logDir,
      Level logLevel, Integer logLimit, Integer logCount, String initFileName) {
    this.historyFileName = historyFileName;
    this.defaultPrompt = defaultPrompt;
    this.historySize = historySize;

    if (initFileName == null) {
      this.initFileName = searchForInitFileName();
    } else {
      this.initFileName = initFileName;
    }

    // Logger properties
    if (logDir == null) {
      this.logDir = System.getProperty(LOG_DIR_PROPERTY, ".");
    } else {
      this.logDir = logDir;
    }
    if (logLevel == null) {
      this.logLevel =
          getLogLevel(System.getProperty(LOG_LEVEL_PROPERTY, DEFAULT_LOGLEVEL.getName()));
    } else {
      this.logLevel = logLevel;
    }
    if (logLimit == null) {
      logFileSizeLimit = getParsedOrDefault(System.getProperty(LOG_FILE_SIZE_LIMIT_PROPERTY),
          LOG_FILE_SIZE_LIMIT_PROPERTY, DEFAULT_LOGFILE_SIZE_LIMIT);
    } else {
      logFileSizeLimit = logLimit;
    }
    if (logCount == null) {
      // validation & correction to default is done in getLogFileCount()
      logFileDiskLimit = getParsedOrDefault(System.getProperty(LOG_DISK_SPACE_LIMIT_PROPERTY),
          LOG_DISK_SPACE_LIMIT_PROPERTY, DEFAULT_LOGFILE_DISK_USAGE);
    } else {
      logFileDiskLimit = logCount;
    }
  }

  public String getHistoryFileName() {
    return historyFileName;
  }

  public String getInitFileName() {
    return initFileName;
  }

  public String getDefaultPrompt() {
    return defaultPrompt;
  }

  public int getHistorySize() {
    return historySize;
  }

  public String getLogFilePath() {
    return IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(logDir, "gfsh-%u_%g.log"));
  }

  public Level getLogLevel() {
    return logLevel;
  }

  public int getLogFileSizeLimit() {
    return logFileSizeLimit;
  }

  protected int getLogFileDiskLimit() {
    return logFileDiskLimit;
  }

  public int getLogFileCount() {
    int logCount;
    try {
      logCount = getLogFileSizeLimit() / getLogFileDiskLimit();
      logCount = logCount >= 1 ? logCount : 1;
    } catch (java.lang.ArithmeticException e) { // for divide by zero
      logCount = 1;
    }
    return logCount;
  }

  public boolean isLoggingEnabled() {
    // keep call for getLogLevel() instead of logLevel for inheritance
    return !Level.OFF.equals(getLogLevel());
  }

  private String getLoggerConfig() {

    return "init-file=" + (getInitFileName() == null ? "" : getInitFileName())
        + Gfsh.LINE_SEPARATOR
        + "log-file=" + getLogFilePath() + Gfsh.LINE_SEPARATOR
        + "log-level=" + getLogLevel().getName() + Gfsh.LINE_SEPARATOR
        + "log-file-size-limit=" + getLogFileSizeLimit() + Gfsh.LINE_SEPARATOR
        + "log-disk-space-limit=" + getLogFileDiskLimit() + Gfsh.LINE_SEPARATOR
        + "log-count=" + getLogFileCount() + Gfsh.LINE_SEPARATOR;
  }

  public boolean isTestConfig() {
    return false;
  }

  public boolean isANSISupported() {
    return !Boolean.getBoolean("gfsh.disable.color");
  }

  private static Level getLogLevel(final String logLevelString) {
    try {
      String logLevelAsString = StringUtils.isBlank(logLevelString) ? "" : logLevelString.trim(); // trim
                                                                                                  // spaces
                                                                                                  // if
                                                                                                  // any
      // To support level NONE, used by GemFire
      if ("NONE".equalsIgnoreCase(logLevelAsString)) {
        logLevelAsString = Level.OFF.getName();
      }
      // To support level ERROR, used by GemFire, fall to WARNING
      if ("ERROR".equalsIgnoreCase(logLevelAsString)) {
        logLevelAsString = Level.WARNING.getName();
      }
      return Level.parse(logLevelAsString.toUpperCase());
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      return DEFAULT_LOGLEVEL;
    }
  }

  private static String getHomeGemFireDirectory() {
    String userHome = System.getProperty("user.home");
    String homeDirPath = userHome + "/.geode";
    File alternateDir = new File(homeDirPath);
    if (!alternateDir.exists()) {
      if (!alternateDir.mkdirs()) {
        homeDirPath = ".";
      }
    }
    return homeDirPath;
  }

  private static int getParsedOrDefault(final String numberString, final String parseValueFor,
      final int defaultValue) {
    if (numberString == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(numberString);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value \"" + numberString + "\" specified for: \"" + parseValueFor
          + "\". Using default value: \"" + defaultValue + "\".");
      return defaultValue;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + " [initFileName="
        + (getInitFileName() == null ? "" : getInitFileName())
        + ", historyFileName="
        + getHistoryFileName()
        + ", historySize="
        + getHistorySize()
        + ", loggerConfig={"
        + getLoggerConfig() + "}"
        + ", isANSISupported="
        + isANSISupported()
        + "]";
  }

  /*
   * Search for the init file using the system property, then the current directory, then the home
   * directory. It need not exist at all.
   */
  private String searchForInitFileName() {
    String homeDirectoryInitFileName =
        System.getProperty("user.home") + File.separatorChar + DEFAULT_INIT_FILE_NAME;
    String currentDirectoryInitFileName =
        System.getProperty("user.dir") + File.separatorChar + DEFAULT_INIT_FILE_NAME;
    String systemPropertyInitFileName = System.getProperty(INIT_FILE_PROPERTY);

    String[] initFileNames =
        {systemPropertyInitFileName, currentDirectoryInitFileName, homeDirectoryInitFileName};

    for (String initFileName : initFileNames) {
      if (IOUtils.isExistingPathname(initFileName)) {
        return initFileName;
      }
    }

    return null;
  }
}
