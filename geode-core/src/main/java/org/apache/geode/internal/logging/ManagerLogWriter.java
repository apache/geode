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
import java.util.Date;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
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

  public static final String TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "logging.test.fileSizeLimitInKB";

  private final boolean fileSizeLimitInKB;

  private final RollingFileHandler rollingFileHandler;

  private LogConfig cfg = null;

  private LocalLogWriter mainLogger = this;

  private File logDir = null;
  private int mainLogId = -1;
  private int childId = 0;
  private boolean useChildLogging = false;

  /**
   * Set to true when roll is in progress
   */
  private boolean rolling = false;
  private boolean mainLog = true;

  private File activeLogFile = null;

  private boolean started = false;

  /**
   * Creates a writer that logs to <code>System.out</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @throws IllegalArgumentException if level is not in legal range
   */
  public ManagerLogWriter(int level, PrintStream out) {
    this(level, out, null);
  }

  /**
   * Creates a writer that logs to <code>System.out</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param connectionName Name of the connection associated with this logger
   *
   * @throws IllegalArgumentException if level is not in legal range
   *
   * @since GemFire 3.5
   */
  public ManagerLogWriter(int level, PrintStream out, String connectionName) {
    super(level, out, connectionName);
    this.fileSizeLimitInKB = Boolean.getBoolean(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    this.rollingFileHandler = new MainWithChildrenRollingFileHandler();
  }

  /**
   * Sets the config that should be used by this manager to decide how to manage its logging.
   */
  public void setConfig(LogConfig cfg) {
    this.cfg = cfg;
    configChanged();
  }

  /**
   * Call when the config changes at runtime.
   */
  public void configChanged() {
    setLevel(cfg.getLogLevel());
    useChildLogging = cfg.getLogFile() != null && !cfg.getLogFile().equals(new File(""))
        && cfg.getLogFileSizeLimit() != 0;

    if (useChildLogging()) {
      logDir = rollingFileHandler.getParentFile(this.cfg.getLogFile());
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
        switchLogs(this.cfg.getLogFile(), true);
      }
    }
  }

  public File getChildLogFile() {
    return this.activeLogFile;
  }

  public File getLogDir() {
    return this.logDir;
  }

  public int getMainLogId() {
    return this.mainLogId;
  }

  private File getNextChildLogFile() {
    String path = this.cfg.getLogFile().getPath();
    int extIdx = path.lastIndexOf('.');
    String ext = "";
    if (extIdx != -1) {
      ext = path.substring(extIdx);
      path = path.substring(0, extIdx);
    }
    path = path + rollingFileHandler.formatId(mainLogId) + rollingFileHandler.formatId(this.childId)
        + ext;
    this.childId++;
    File result = new File(path);
    if (result.exists()) {
      // try again until a unique name is found
      return getNextChildLogFile();
    } else {
      return result;
    }
  }

  public boolean useChildLogging() {
    return this.useChildLogging;
  }

  private long getLogFileSizeLimit() {
    if (rolling || mainLog) {
      return Long.MAX_VALUE;
    } else {
      long result = cfg.getLogFileSizeLimit();
      if (result == 0) {
        return Long.MAX_VALUE;
      }
      if (this.fileSizeLimitInKB) {
        // use KB instead of MB to speed up log rolling for test purpose
        return result * (1024);
      } else {
        return result * (1024 * 1024);
      }
    }
  }

  private long getLogDiskSpaceLimit() {
    long result = cfg.getLogDiskSpaceLimit();
    return result * (1024 * 1024);
  }

  private String getMetaLogFileName(String baseLogFileName, int mainLogId) {
    String metaLogFile = null;
    int extIdx = baseLogFileName.lastIndexOf('.');
    String ext = "";
    if (extIdx != -1) {
      ext = baseLogFileName.substring(extIdx);
      metaLogFile = baseLogFileName.substring(0, extIdx);
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
          if (mainLog && mainLogger == this && this.cfg.getLogFile() != null) {
            // this is the first child. Save the current output stream
            // so we can log special messages to the main log instead
            // of the current child log.
            String metaLogFile =
                getMetaLogFileName(this.cfg.getLogFile().getPath(), this.mainLogId);
            mainLogger = new LocalLogWriter(INFO_LEVEL,
                new PrintStream(new FileOutputStream(metaLogFile, true), true));
            if (activeLogFile == null) {
              mainLogger.info(LocalizedStrings.ManagerLogWriter_SWITCHING_TO_LOG__0,
                  this.cfg.getLogFile());
            }
          } else {
            mainLogger.info(LocalizedStrings.ManagerLogWriter_ROLLING_CURRENT_LOG_TO_0, newLog);
          }
        }
        boolean renameOK = true;
        String oldName = this.cfg.getLogFile().getAbsolutePath();
        File tmpFile = null;
        if (this.activeLogFile != null) {
          // @todo gregp: is a bug that we get here and try to rename the activeLogFile
          // to a newLog of the same name? This works ok on Unix but on windows fails.
          if (!this.activeLogFile.getAbsolutePath().equals(newLog.getAbsolutePath())) {
            boolean isWindows = false;
            String os = System.getProperty("os.name");
            if (os != null) {
              if (os.indexOf("Windows") != -1) {
                isWindows = true;
              }
            }
            if (isWindows) {
              // For windows to work we need to redirect everything
              // to a temporary file so we can get oldFile closed down
              // so we can rename it. We don't actually write to this tmp file
              File tmpLogDir = rollingFileHandler.getParentFile(this.cfg.getLogFile());
              tmpFile = File.createTempFile("mlw", null, tmpLogDir);
              // close the old print writer down before we do the rename
              PrintStream tmpps = OSProcess.redirectOutput(tmpFile,
                  AlertAppender.getInstance().isAlertingDisabled()/* See #49492 */);
              PrintWriter oldPW = this.setTarget(new PrintWriter(tmpps, true));
              if (oldPW != null) {
                oldPW.close();
              }
            }
            File oldFile = this.activeLogFile;
            renameOK = LogFileUtils.renameAggressively(oldFile, newLog.getAbsoluteFile());
            if (!renameOK) {
              mainLogger
                  .warning("Could not delete original file '" + oldFile + "' after copying to '"
                      + newLog.getAbsoluteFile() + "'. Continuing without rolling.");
            } else {
              renameOK = true;
            }
          }
        }
        this.activeLogFile = new File(oldName);
        // Don't redirect sysouts/syserrs to client log file. See #49492.
        // IMPORTANT: This assumes that only a loner would have sendAlert set to false.
        PrintStream ps = OSProcess.redirectOutput(activeLogFile,
            AlertAppender.getInstance().isAlertingDisabled());
        PrintWriter oldPW = this.setTarget(new PrintWriter(ps, true), this.activeLogFile.length());
        if (oldPW != null) {
          oldPW.close();
        }
        if (tmpFile != null) {
          tmpFile.delete();
        }
        mainLog = newIsMain;
        if (mainLogger == null) {
          mainLogger = this;
        }
        if (!renameOK) {
          mainLogger.warning("Could not rename \"" + this.activeLogFile + "\" to \"" + newLog
              + "\". Continuing without rolling.");
        }
      } catch (IOException ex) {
        mainLogger.warning("Could not open log \"" + newLog + "\" because " + ex);
      }
      checkDiskSpace(this.activeLogFile);
    } finally {
      rolling = false;
    }
  }

  /** notification from manager that the output file is being closed */
  public void closingLogFile() {
    OutputStream out = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        // --> /dev/null
      }
    };
    close();
    if (mainLogger != null) {
      mainLogger.close();
    }
    PrintWriter pw = this.setTarget(new PrintWriter(out, true));
    if (pw != null) {
      pw.close();
    }
  }

  public static File getLogNameForOldMainLog(File log, boolean useOldFile) {
    /*
     * this is just searching for the existing logfile name we need to search for meta log file name
     *
     */
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

    File result = null;
    int childId = rollingFileHandler.calcNextChildId(log, previousMainId > 0 ? previousMainId : 0);
    StringBuffer buf = new StringBuffer(log.getPath());
    int insertIdx = buf.lastIndexOf(".");
    if (insertIdx == -1) {
      buf.append(rollingFileHandler.formatId(previousMainId))
          .append(rollingFileHandler.formatId(childId));
    } else {
      buf.insert(insertIdx, rollingFileHandler.formatId(childId));
      buf.insert(insertIdx, rollingFileHandler.formatId(previousMainId));
    }
    result = new File(buf.toString());
    return result;
  }

  private void checkDiskSpace(File newLog) {
    rollingFileHandler.checkDiskSpace("log", newLog, getLogDiskSpaceLimit(), logDir, mainLogger);
  }

  public void rollLog() {
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
      switchLogs(this.cfg.getLogFile(), true);
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
  public String put(int msgLevel, Date msgDate, String connectionName, String threadName, long tid,
      String msg, String exceptionText) {

    String result = null; // This seems to workaround a javac bug
    result = super.put(msgLevel, msgDate, connectionName, threadName, tid, msg, exceptionText);
    return result;
  }

  @Override
  public void writeFormattedMessage(String s) {
    rollLogIfFull();
    super.writeFormattedMessage(s);
  }
}
