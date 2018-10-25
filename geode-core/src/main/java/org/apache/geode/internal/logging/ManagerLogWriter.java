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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.util.LogFileUtils;

/**
 * Implementation of {@link LogWriterI18n} for distributed system members. It's just like
 * {@link LocalLogWriter} except it has support for rolling and alerts.
 *
 * @since Geode 1.0
 */
public class ManagerLogWriter extends LocalLogWriter {

  private static final String TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "logging.test.fileSizeLimitInKB";

  private final boolean fileSizeLimitInKB;

  private final RollingFileHandler rollingFileHandler;

  private LogConfig config;

  private LocalLogWriter mainLogWriter = this;

  private File logDir;
  private int mainLogId = -1;
  private int childId;
  private boolean useChildLogging;

  /**
   * Set to true when roll is in progress
   */
  private boolean rolling;
  private boolean mainLog = true;

  private File activeLogFile;

  private boolean started;

  /**
   * Creates a writer that logs to <code>printStream</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param printStream is the stream that message will be printed to.
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public ManagerLogWriter(int level, PrintStream printStream) {
    this(level, printStream, null);
  }

  /**
   * Creates a writer that logs to <code>printStream</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param printStream is the stream that message will be printed to.
   * @param connectionName Name of the connection associated with this logger
   *
   * @throws IllegalArgumentException if level is not in legal range
   *
   * @since GemFire 3.5
   */
  public ManagerLogWriter(int level, PrintStream printStream, String connectionName) {
    super(level, printStream, connectionName);
    fileSizeLimitInKB = Boolean.getBoolean(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    rollingFileHandler = new MainWithChildrenRollingFileHandler();
  }

  /**
   * Sets the config that should be used by this manager to decide how to manage its logging.
   */
  public void setConfig(LogConfig config) {
    this.config = config;
    configChanged();
  }

  /**
   * Call when the config changes at runtime.
   */
  public void configChanged() {
    setLevel(config.getLogLevel());
    useChildLogging = config.getLogFile() != null && !config.getLogFile().equals(new File(""))
        && config.getLogFileSizeLimit() != 0;

    if (useChildLogging()) {
      logDir = rollingFileHandler.getParentFile(config.getLogFile());
      // let archive id always follow main log id, not the vice versa
      // e.g. in getArchiveName(), it's using mainArchiveId = calcNextMainId(archiveDir);
      // This is the only place we assign mainLogId.
      // mainLogId is only referenced when useChildLogging==true
      mainLogId = rollingFileHandler.calcNextMainId(logDir, true);
    }
    if (started) {
      if (useChildLogging()) {
        if (mainLog) {
          rollLog();
        }
      } else {
        switchLogs(config.getLogFile(), true);
      }
    }
  }

  public File getChildLogFile() {
    return activeLogFile;
  }

  public File getLogDir() {
    return logDir;
  }

  public int getMainLogId() {
    return mainLogId;
  }

  private File getNextChildLogFile() {
    String path = config.getLogFile().getPath();
    int extIndex = path.lastIndexOf('.');
    String ext = "";
    if (extIndex != -1) {
      ext = path.substring(extIndex);
      path = path.substring(0, extIndex);
    }
    path = path + rollingFileHandler.formatId(mainLogId) + rollingFileHandler.formatId(childId)
        + ext;
    childId++;
    File result = new File(path);
    if (result.exists()) {
      // try again until a unique name is found
      return getNextChildLogFile();
    } else {
      return result;
    }
  }

  public boolean useChildLogging() {
    return useChildLogging;
  }

  private long getLogFileSizeLimit() {
    if (rolling || mainLog) {
      return Long.MAX_VALUE;
    } else {
      long result = config.getLogFileSizeLimit();
      if (result == 0) {
        return Long.MAX_VALUE;
      }
      if (fileSizeLimitInKB) {
        // use KB instead of MB to speed up log rolling for test purpose
        return result * 1024;
      } else {
        return result * (1024 * 1024);
      }
    }
  }

  private long getLogDiskSpaceLimit() {
    long result = config.getLogDiskSpaceLimit();
    return result * (1024 * 1024);
  }

  private String getMetaLogFileName(String baseLogFileName, int mainLogId) {
    String metaLogFile = null;
    int extIndex = baseLogFileName.lastIndexOf('.');
    String ext = "";
    if (extIndex != -1) {
      ext = baseLogFileName.substring(extIndex);
      metaLogFile = baseLogFileName.substring(0, extIndex);
    }
    String fileName = new File(metaLogFile).getName();
    String parent = new File(metaLogFile).getParent();

    metaLogFile = "meta-" + fileName + rollingFileHandler.formatId(mainLogId) + ext;
    if (parent != null) {
      metaLogFile = parent + File.separator + metaLogFile;
    }
    return metaLogFile;
  }

  private synchronized void switchLogs(File newLog, boolean newIsMain) {
    rolling = true;
    try {
      try {
        if (!newIsMain) {
          if (mainLog && mainLogWriter == this && config.getLogFile() != null) {
            // this is the first child. Save the current output stream
            // so we can log special messages to the main log instead
            // of the current child log.
            String metaLogFile =
                getMetaLogFileName(config.getLogFile().getPath(), mainLogId);
            mainLogWriter = new LocalLogWriter(INFO_LEVEL,
                new PrintStream(new FileOutputStream(metaLogFile, true), true));
            if (activeLogFile == null) {
              mainLogWriter.info(String.format("Switching to log %s",
                  config.getLogFile()));
            }
          } else {
            mainLogWriter.info(String.format("Rolling current log to %s", newLog));
          }
        }
        boolean renameOK = true;
        String oldName = config.getLogFile().getAbsolutePath();
        File tempFile = null;
        if (activeLogFile != null) {
          // is a bug that we get here and try to rename the activeLogFile
          // to a newLog of the same name? This works ok on Unix but on windows fails.
          if (!activeLogFile.getAbsolutePath().equals(newLog.getAbsolutePath())) {
            boolean isWindows = false;
            String os = System.getProperty("os.name");
            if (os != null) {
              if (os.contains("Windows")) {
                isWindows = true;
              }
            }
            if (isWindows) {
              // For windows to work we need to redirect everything
              // to a temporary file so we can get oldFile closed down
              // so we can rename it. We don't actually write to this tmp file
              File tempLogDir = rollingFileHandler.getParentFile(config.getLogFile());
              tempFile = File.createTempFile("mlw", null, tempLogDir);
              // close the old print writer down before we do the rename
              PrintStream tempPrintStream = OSProcess.redirectOutput(tempFile,
                  AlertAppender.getInstance().isAlertingDisabled()/* See #49492 */);
              PrintWriter oldPrintWriter = setTarget(new PrintWriter(tempPrintStream, true));
              if (oldPrintWriter != null) {
                oldPrintWriter.close();
              }
            }
            File oldFile = activeLogFile;
            renameOK = LogFileUtils.renameAggressively(oldFile, newLog.getAbsoluteFile());
            if (!renameOK) {
              mainLogWriter
                  .warning("Could not delete original file '" + oldFile + "' after copying to '"
                      + newLog.getAbsoluteFile() + "'. Continuing without rolling.");
            } else {
              renameOK = true;
            }
          }
        }
        activeLogFile = new File(oldName);
        // Don't redirect sysouts/syserrs to client log file. See #49492.
        // IMPORTANT: This assumes that only a loner would have sendAlert set to false.
        PrintStream printStream = OSProcess.redirectOutput(activeLogFile,
            AlertAppender.getInstance().isAlertingDisabled());
        PrintWriter oldPrintWriter =
            setTarget(new PrintWriter(printStream, true), activeLogFile.length());
        if (oldPrintWriter != null) {
          oldPrintWriter.close();
        }
        if (tempFile != null) {
          tempFile.delete();
        }
        mainLog = newIsMain;
        if (mainLogWriter == null) {
          mainLogWriter = this;
        }
        if (!renameOK) {
          mainLogWriter.warning("Could not rename \"" + activeLogFile + "\" to \"" + newLog
              + "\". Continuing without rolling.");
        }
      } catch (IOException ex) {
        mainLogWriter.warning("Could not open log \"" + newLog + "\" because " + ex);
      }
      checkDiskSpace(activeLogFile);
    } finally {
      rolling = false;
    }
  }

  /**
   * notification from manager that the output file is being closed
   */
  public void closingLogFile() {
    OutputStream nullOutputStream = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        // --> /dev/null
      }
    };
    close();
    if (mainLogWriter != null) {
      mainLogWriter.close();
    }
    PrintWriter printWriter = setTarget(new PrintWriter(nullOutputStream, true));
    if (printWriter != null) {
      printWriter.close();
    }
  }

  public static File getLogNameForOldMainLog(File log, boolean useOldFile) {
    // this is just searching for the existing logfile name we need to search for meta log file name
    RollingFileHandler rollingFileHandler = new MainWithChildrenRollingFileHandler();
    File dir = rollingFileHandler.getParentFile(log.getAbsoluteFile());
    int previousMainId = rollingFileHandler.calcNextMainId(dir, true);
    if (useOldFile) {
      if (previousMainId > 0) {
        previousMainId--;
      }
    }
    if (previousMainId == 0) {
      previousMainId = 1;
    }

    int childId = rollingFileHandler.calcNextChildId(log, previousMainId > 0 ? previousMainId : 0);
    StringBuilder sb = new StringBuilder(log.getPath());
    int insertIdx = sb.lastIndexOf(".");
    if (insertIdx == -1) {
      sb.append(rollingFileHandler.formatId(previousMainId))
          .append(rollingFileHandler.formatId(childId));
    } else {
      sb.insert(insertIdx, rollingFileHandler.formatId(childId));
      sb.insert(insertIdx, rollingFileHandler.formatId(previousMainId));
    }
    return new File(sb.toString());
  }

  private void checkDiskSpace(File newLog) {
    rollingFileHandler.checkDiskSpace("log", newLog, getLogDiskSpaceLimit(), logDir, mainLogWriter);
  }

  private void rollLog() {
    rollLog(false);
  }

  private void rollLogIfFull() {
    rollLog(true);
  }

  private void rollLog(boolean ifFull) {
    if (!useChildLogging()) {
      return;
    }
    synchronized (this) {
      // need to do the activeLogFull call while synchronized
      if (ifFull && !activeLogFull()) {
        return;
      }
      switchLogs(getNextChildLogFile(), false);
    }
  }

  /**
   * Called when manager is done starting up. This is when a child log will be started if rolling is
   * configured.
   */
  public void startupComplete() {
    started = true;
    rollLog();
  }

  /**
   * Called when manager starts shutting down. This is when any current child log needs to be closed
   * and the rest of the logging reverted to the main.
   */
  public void shuttingDown() {
    if (useChildLogging()) {
      switchLogs(config.getLogFile(), true);
    }
  }

  private boolean activeLogFull() {
    long limit = getLogFileSizeLimit();
    if (limit == Long.MAX_VALUE) {
      return false;
    } else {
      return getBytesLogged() >= limit;
    }
  }

  @Override
  public void writeFormattedMessage(String message) {
    rollLogIfFull();
    super.writeFormattedMessage(message);
  }
}
