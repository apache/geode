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

import static org.apache.geode.internal.logging.ManagerLogWriter.getLogNameForOldMainLog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.util.LogFileUtils;
import org.apache.geode.logging.spi.LogConfig;
import org.apache.geode.statistics.StatisticsConfig;

/**
 * Factory for creating a {@link ManagerLogWriter}.
 */
public class ManagerLogWriterFactory {

  private static final Logger LOGGER = StatusLogger.getLogger();

  private final LogFileRolloverDetails logFileRolloverDetails;

  private boolean security;
  private boolean appendLog;

  public ManagerLogWriterFactory() {
    logFileRolloverDetails = new LogFileRolloverDetails();
  }

  public ManagerLogWriterFactory setSecurity(final boolean value) {
    security = value;
    return this;
  }

  public ManagerLogWriterFactory setAppendLog(final boolean value) {
    appendLog = value;
    return this;
  }

  public LogFileRolloverDetails getLogFileRolloverDetails() {
    return logFileRolloverDetails;
  }

  public ManagerLogWriter create(final LogConfig logConfig, final StatisticsConfig statsConfig) {
    File logFile = getLogFile(logConfig);
    if (logFile == null || logFile.equals(new File(""))) {
      return null;
    }

    // if logFile exists attempt to rename it for rolling
    if (logFile.exists()) {
      boolean useChildLogging = useChildLogging(logConfig);
      boolean statArchivesRolling = statArchivesRolling(statsConfig);

      if (!appendLog || useChildLogging || statArchivesRolling) {
        File oldMain =
            getLogNameForOldMainLog(logFile, security || useChildLogging || statArchivesRolling);

        boolean succeeded = LogFileUtils.renameAggressively(logFile, oldMain);

        if (succeeded) {
          logFileRolloverDetails.message = String.format("Renamed old log file to %s.", oldMain);
        } else {
          logFileRolloverDetails.warning = true;
          logFileRolloverDetails.message =
              String.format("Could not rename %s to %s.", logFile, oldMain);
        }
      }
    }

    ManagerLogWriter logWriter = newManagerLogWriter(logConfig, createPrintStream(logFile));
    logWriter.setConfig(logConfig);
    redirectOutput(logConfig);
    return logWriter;
  }

  @VisibleForTesting
  File getLogFile(final LogConfig config) {
    if (security) {
      return config.getSecurityLogFile();
    } else {
      return config.getLogFile();
    }
  }

  @VisibleForTesting
  int getLogLevel(final LogConfig config) {
    if (security) {
      return config.getSecurityLogLevel();
    } else {
      return config.getLogLevel();
    }
  }

  private boolean useChildLogging(final LogConfig config) {
    return config.getLogFile() != null
        && !config.getLogFile().equals(new File("")) && config.getLogFileSizeLimit() != 0;
  }

  private boolean statArchivesRolling(final StatisticsConfig config) {
    return config.getStatisticArchiveFile() != null
        && !config.getStatisticArchiveFile().equals(new File(""))
        && config.getArchiveFileSizeLimit() != 0 && config.getStatisticSamplingEnabled();
  }

  private PrintStream createPrintStream(final File logFile) {
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(logFile, true);
    } catch (FileNotFoundException e) {
      String message = String.format("Could not open log file %s.", logFile);
      throw new GemFireIOException(message, e);
    }
    return new PrintStream(fos);
  }

  private ManagerLogWriter newManagerLogWriter(final LogConfig config,
      final PrintStream printStream) {
    if (security) {
      return new SecurityManagerLogWriter(getLogLevel(config), printStream, config.getName(),
          config.isLoner());
    } else {
      return new ManagerLogWriter(getLogLevel(config), printStream, config.getName(),
          config.isLoner());
    }
  }

  private void redirectOutput(final LogConfig config) {
    if (ProcessLauncherContext.isRedirectingOutput()) {
      try {
        OSProcess.redirectOutput(config.getLogFile());
      } catch (IOException e) {
        LOGGER.error("Unable to redirect output to {}", config.getLogFile(), e);
      }
    }
  }

  public static class LogFileRolloverDetails {

    private String message;
    private boolean warning;

    public String getMessage() {
      return message;
    }

    public boolean isWarning() {
      return warning;
    }

    public boolean exists() {
      return message != null;
    }
  }
}
