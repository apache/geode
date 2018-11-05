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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.ManagerLogWriter;
import org.apache.geode.internal.logging.SecurityManagerLogWriter;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.util.LogFileUtils;

/**
 * Holds on to one or many instances of LogWriterAppender and provides convenience methods for
 * controlling their lifecycles.
 *
 */
public class LogWriterAppenders {
  public static final String APPEND_TO_LOG_FILE = DistributionConfig.GEMFIRE_PREFIX + "append-log";
  private static final boolean ALLOW_REDIRECT = true;

  public enum Identifier {
    MAIN(false), SECURITY(true);
    private final boolean isSecure;

    Identifier(final boolean isSecure) {
      this.isSecure = isSecure;
    }

    public boolean isSecure() {
      return this.isSecure;
    }
  }

  private static Map<Identifier, LogWriterAppender> appenders =
      new HashMap<Identifier, LogWriterAppender>();
  private static Map<Identifier, AtomicInteger> references =
      new HashMap<Identifier, AtomicInteger>();

  /**
   * Returns the named LogWriterAppender or null if it does not exist.
   */
  public static synchronized LogWriterAppender getAppender(final Identifier id) {
    return appenders.get(id);
  }

  /**
   * Returns the named LogWriterAppender or creates it if necessary.
   */
  public static synchronized LogWriterAppender getOrCreateAppender(final Identifier id,
      final boolean appendToFile, final boolean isLoner, final LogConfig config,
      final boolean logConfig) {
    LogWriterAppender appender = appenders.get(id);
    if (appender == null) {
      appender = createLogWriterAppender(appendToFile, isLoner, id.isSecure(), config, logConfig);
      appenders.put(id, appender);
      references.put(id, new AtomicInteger(1));
    } else {
      appender.setConfig(config);
      references.get(id).incrementAndGet();
    }
    return appender;
  }

  /**
   * Returns the named LogWriterAppender or creates it if necessary.
   */
  public static LogWriterAppender getOrCreateAppender(final Identifier id, final boolean isLoner,
      final LogConfig config, final boolean logConfig) {
    final boolean appendToFile = Boolean.getBoolean(APPEND_TO_LOG_FILE);
    return getOrCreateAppender(id, appendToFile, isLoner, config, logConfig);
  }

  /**
   * Creates the log writer appender for a distributed system based on the system's parsed
   * configuration. The initial banner and messages are also entered into the log by this method.
   *
   * @param isLoner Whether the distributed system is a loner or not
   * @param isSecurity Whether a log for security related messages has to be created
   * @param config The DistributionConfig for the target distributed system
   * @param logConfig if true log the configuration
   * @throws GemFireIOException if the log file can't be opened for writing
   */
  static LogWriterAppender createLogWriterAppender(final boolean appendToFile,
      final boolean isLoner, final boolean isSecurity, final LogConfig config,
      final boolean logConfig) {

    final boolean isDistributionConfig = config instanceof DistributionConfig;
    final DistributionConfig dsConfig = isDistributionConfig ? (DistributionConfig) config : null;
    File logFile = config.getLogFile();
    String firstMsg = null;
    boolean firstMsgWarning = false;

    AlertAppender.getInstance().setAlertingDisabled(isLoner);

    // security-log-file is specified in DistributionConfig
    if (isSecurity) {
      if (isDistributionConfig) {
        File tmpLogFile = dsConfig.getSecurityLogFile();
        if (tmpLogFile != null && !tmpLogFile.equals(new File(""))) {
          logFile = tmpLogFile;
        }
      } else {
        throw new IllegalArgumentException("DistributionConfig is expected for SecurityLogWriter");
      }
    }

    // log-file is NOT specified in DistributionConfig
    if (logFile == null || logFile.equals(new File(""))) {
      // out = System.out;
      return null;
    }

    // log-file is specified in DistributionConfig

    // if logFile exists attempt to rename it for rolling
    if (logFile.exists()) {
      final boolean useChildLogging = config.getLogFile() != null
          && !config.getLogFile().equals(new File("")) && config.getLogFileSizeLimit() != 0;
      final boolean statArchivesRolling =
          isDistributionConfig && dsConfig.getStatisticArchiveFile() != null
              && !dsConfig.getStatisticArchiveFile().equals(new File(""))
              && dsConfig.getArchiveFileSizeLimit() != 0 && dsConfig.getStatisticSamplingEnabled();

      if (!appendToFile || useChildLogging || statArchivesRolling) { // check useChildLogging for
                                                                     // bug 50659
        final File oldMain = ManagerLogWriter.getLogNameForOldMainLog(logFile,
            isSecurity || useChildLogging || statArchivesRolling);
        final boolean succeeded = LogFileUtils.renameAggressively(logFile, oldMain);

        if (succeeded) {
          firstMsg = String.format("Renamed old log file to %s.",
              oldMain);
        } else {
          firstMsgWarning = true;
          firstMsg = String.format("Could not rename %s to %s.",
              logFile, oldMain);
        }
      }
    }

    // create a FileOutputStream to the logFile
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(logFile, true);
    } catch (FileNotFoundException ex) {
      String s = String.format("Could not open log file %s.",
          logFile);
      throw new GemFireIOException(s, ex);
    }
    final PrintStream out = new PrintStream(fos);

    // create the ManagerLogWriter that LogWriterAppender will wrap
    ManagerLogWriter mlw = null;
    String logWriterLoggerName = null;
    if (isSecurity) {
      mlw = new SecurityManagerLogWriter(dsConfig.getSecurityLogLevel(), out, config.getName());
      logWriterLoggerName = LogService.SECURITY_LOGGER_NAME;
    } else {
      mlw = new ManagerLogWriter(config.getLogLevel(), out, config.getName());
      logWriterLoggerName = LogService.MAIN_LOGGER_NAME;
    }

    mlw.setConfig(config);
    // if (mlw.infoEnabled()) { -- skip here and instead do this in LogWriterFactory when creating
    // the LogWriterLogger
    // if (!isLoner /* do this on a loner to fix bug 35602 */
    // || !Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER)) {
    // mlw.info(Banner.getString(null));
    // }
    // }

    AppenderContext[] appenderContext = new AppenderContext[1];
    if (isSecurity) {
      appenderContext[0] = LogService.getAppenderContext(LogService.SECURITY_LOGGER_NAME);
    } else {
      appenderContext[0] = LogService.getAppenderContext(); // ROOT or
                                                            // gemfire.logging.appenders.LOGGER
    }

    // create the LogWriterAppender that delegates to ManagerLogWriter;
    final LogWriterAppender appender =
        LogWriterAppender.create(appenderContext, logWriterLoggerName, mlw, fos);

    // remove stdout appender from MAIN_LOGGER_NAME only if isUsingGemFireDefaultConfig -- see
    // #51819
    if (!isSecurity && LogService.MAIN_LOGGER_NAME.equals(logWriterLoggerName)
        && LogService.isUsingGemFireDefaultConfig()) {
      LogService.removeConsoleAppender();
    }

    // log the first msg about renaming logFile for rolling if it pre-existed
    final InternalLogWriter logWriter = mlw;
    if (firstMsg != null) {
      if (firstMsgWarning) {
        logWriter.warning(firstMsg);
      } else {
        logWriter.info(firstMsg);
      }
    }

    // log the config
    if (logConfig) {
      if (!isLoner) {
        // LOG:CONFIG: changed from config to info
        logWriter.info(String.format("Startup Configuration: %s",
            config.toLoggerString()));
      }
    }

    // LOG: do NOT allow redirectOutput
    if (ALLOW_REDIRECT) {
      // fix #46493 by moving redirectOutput invocation here
      if (ProcessLauncherContext.isRedirectingOutput()) {
        try {
          OSProcess.redirectOutput(config.getLogFile());
        } catch (IOException e) {
          logWriter.error(e);
          // throw new GemFireIOException("Unable to redirect output to " + config.getLogFile(), e);
        }
      }
    }

    return appender;
  }

  public static synchronized void startupComplete(final Identifier id) {
    LogWriterAppender appender = appenders.get(id);
    if (appender != null) {
      appender.startupComplete();
    }
  }

  public static synchronized void destroy(final Identifier id) {
    LogWriterAppender appender = appenders.get(id);
    if (appender == null) {
      return;
    }
    AtomicInteger count = references.get(id);
    if (count == null) {
      throw new IllegalStateException("Count is null for " + id);
    }
    if (count.get() < 0) {
      throw new IllegalStateException(
          "Count is non-positive integer for " + id + ": " + count.get());
    }
    if (count.decrementAndGet() > 0) {
      return;
    } else {
      appenders.remove(id);
      references.remove(id);
      appender.destroy();
    }
  }

  public static synchronized void stop(final Identifier id) {
    LogWriterAppender appender = appenders.get(id);
    if (appender != null) {
      appender.stop();
    }
  }

  public static synchronized void configChanged(final Identifier id) {
    LogWriterAppender appender = appenders.get(id);
    if (appender != null) {
      appender.configChanged();
    }
  }
}
