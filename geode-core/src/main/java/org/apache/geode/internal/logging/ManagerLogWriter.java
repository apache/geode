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
package org.apache.geode.internal.logging;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.util.LogFileUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.regex.Pattern;

/**
  * Implementation of {@link LogWriterI18n} for distributed system members.
  * Its just like {@link LocalLogWriter} except it has support for rolling
  * and alerts. 
  *
  * @since Geode 1.0
  */
public class ManagerLogWriter extends LocalLogWriter  {

  public static final String TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "logging.test.fileSizeLimitInKB";

  private final boolean fileSizeLimitInKB;
  
  private LogConfig cfg = null;
  private DistributionManager dm = null;

    // Constructors
    /**
     * Creates a writer that logs to <code>System.out</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public ManagerLogWriter(int level, PrintStream out) {
	this(level, out, null);
    }

    /**
     * Creates a writer that logs to <code>System.out</code>.
     *
     * @param level 
     *        only messages greater than or equal to this value will
     *        be logged. 
     * @param connectionName
     *        Name of the connection associated with this logger
     *
     * @throws IllegalArgumentException if level is not in legal range
     *
     * @since GemFire 3.5
     */
    public ManagerLogWriter(int level, PrintStream out,
                            String connectionName) {
	super(level, out, connectionName);
	this.fileSizeLimitInKB = Boolean.getBoolean(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    }
    
    private LocalLogWriter mainLogger = this;

    /**
     * Gets the logger that writes to the main log file.
     * This logger may differ from the current logger when rolling
     * logs are used.
     */
    public LogWriterI18n getMainLogger() {
      return this.mainLogger;
    }
  
    /**
     * Sets the config that should be used by this manager to decide
     * how to manage its logging.
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
      useChildLogging = cfg.getLogFile() != null
        && !cfg.getLogFile().equals(new File(""))
        && cfg.getLogFileSizeLimit() != 0;

      if (useChildLogging()) {
        childLogPattern = getLogPattern(this.cfg.getLogFile().getName());
        logDir = getParentFile(this.cfg.getLogFile());
        // let archive id always follow main log id, not the vice versa
        // e.g. in getArchiveName(), it's using mainArchiveId = calcNextMainId(archiveDir);
        // This is the only place we assign mainLogId. 
        // mainLogId is only referenced when useChildLogging==true
        mainLogId = calcNextMainId(logDir, true);
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

  private Pattern childLogPattern = null;
  private File logDir = null;
  private int mainLogId = -1;
  private int childId = 0;


  public File getLogDir() {
    return this.logDir;
  }

  public int getMainLogId() {
    return this.mainLogId;
  }
  
  public static String formatId(int id) {
    StringBuffer result = new StringBuffer(10);
    result.append('-');
    if (id < 10) {
      result.append('0');
    }
    result.append(id);
    return result.toString();
  }
    private File getNextChildLogFile() {
      String path = this.cfg.getLogFile().getPath();
      int extIdx = path.lastIndexOf('.');
      String ext = "";
      if (extIdx != -1) {
        ext = path.substring(extIdx);
        path = path.substring(0, extIdx);
      }
      path = path + formatId(mainLogId) + formatId(this.childId) + ext;
      this.childId++;
      File result = new File(path);
      if (result.exists()) {
        // try again until a unique name is found
        return getNextChildLogFile();
      } else {
        return result;
      }
    }

    private boolean useChildLogging = false;
    public boolean useChildLogging() {
      return this.useChildLogging;
    }
    /**
     * Set to true when roll is in progress
     */
    private boolean rolling = false; 
    private boolean mainLog = true; 

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
          return result * (1024*1024);
        }
      }
    }
    private long getLogDiskSpaceLimit() {
      long result = cfg.getLogDiskSpaceLimit();
      return result * (1024*1024);
    }

    
    private static String getMetaLogFileName(String baseLogFileName, int mainLogId) {
      String metaLogFile = null;
      int extIdx = baseLogFileName.lastIndexOf('.');
      String ext = "";
      if (extIdx != -1) {
        ext = baseLogFileName.substring(extIdx);
        metaLogFile = baseLogFileName.substring(0, extIdx);
      }
      String fileName = new File(metaLogFile).getName();
      String parent = new File(metaLogFile).getParent();
      
      metaLogFile = "meta-"+ fileName + formatId(mainLogId) + ext;
      if (parent != null) {
        metaLogFile = parent + File.separator + metaLogFile;
      }
      return metaLogFile;
    }
    
    private File activeLogFile = null;
    private synchronized void switchLogs(File newLog, boolean newIsMain) {
      rolling = true;
      try {
        try {
          if (!newIsMain) {
            if (mainLog && mainLogger == this && this.cfg.getLogFile() != null) {
              // this is the first child. Save the current output stream
              // so we can log special messages to the main log instead
              // of the current child log.
              String metaLogFile = getMetaLogFileName(this.cfg.getLogFile().getPath(), this.mainLogId);
              mainLogger = new LocalLogWriter(INFO_LEVEL, new PrintStream(new FileOutputStream(metaLogFile, true), true));
              if(activeLogFile==null) {
                mainLogger.info(LocalizedStrings.ManagerLogWriter_SWITCHING_TO_LOG__0, this.cfg.getLogFile());
              }
            } else {
              mainLogger.info(LocalizedStrings.ManagerLogWriter_ROLLING_CURRENT_LOG_TO_0, newLog);
            }
          }
          boolean renameOK = true;
          String oldName = this.cfg.getLogFile().getAbsolutePath();
          File tmpFile = null;
          if(this.activeLogFile!=null) {
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
                File tmpLogDir = getParentFile(this.cfg.getLogFile());
                tmpFile = File.createTempFile("mlw", null, tmpLogDir);
                // close the old guy down before we do the rename
                PrintStream tmpps = OSProcess.redirectOutput(tmpFile, AlertAppender.getInstance().isAlertingDisabled()/* See #49492 */);
                PrintWriter oldPW = this.setTarget(new PrintWriter(tmpps,true));
                if (oldPW != null) {
                  oldPW.close();
                }
              }
              File oldFile = this.activeLogFile;
              renameOK = LogFileUtils.renameAggressively(oldFile,newLog.getAbsoluteFile());
              if(!renameOK) {
                mainLogger.warning("Could not delete original file '" + oldFile +
                          "' after copying to '" + newLog.getAbsoluteFile() + "'. Continuing without rolling.");
              } else {
	       renameOK = true;
	      } 
            }
          }
          this.activeLogFile = new File(oldName);
          // Don't redirect sysouts/syserrs to client log file. See #49492.
          // IMPORTANT: This assumes that only a loner would have sendAlert set to false.
          PrintStream ps = OSProcess.redirectOutput(activeLogFile, AlertAppender.getInstance().isAlertingDisabled());
          PrintWriter oldPW = this.setTarget(new PrintWriter(ps, true), this.activeLogFile.length());
          if (oldPW != null) {
            oldPW.close();
          }
          if (tmpFile != null) {
            tmpFile.delete();
          }
          mainLog = newIsMain;
          if (mainLogger==null) {
            mainLogger = this;
          }
          if (!renameOK) {
            mainLogger.warning("Could not rename \"" + this.activeLogFile
                               + "\" to \"" + newLog + "\". Continuing without rolling.");
          }
        } catch (IOException ex) {
          mainLogger.warning("Could not open log \"" + newLog
                             + "\" because " + ex);
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
      if(mainLogger!=null) {
         mainLogger.close();
      }
      PrintWriter pw = this.setTarget(new PrintWriter(out, true));
      if(pw!=null) {
        pw.close();
      }
    }

    /**
     * as a fix for bug #41474 we use "." if getParentFile returns null
     */
    private static File getParentFile(File f) {
      File tmp = f.getAbsoluteFile().getParentFile();
      if (tmp == null) {
        tmp = new File(".");
      }
      return tmp;
    }
    
    
    public static File getLogNameForOldMainLog(File log, boolean useOldFile) {
      /*
       * this is just searching for the existing logfile name
       * we need to search for meta log file name
       * 
       */
      File dir = getParentFile(log.getAbsoluteFile());
      int previousMainId = calcNextMainId(dir, true);
      if (useOldFile) {
        if(previousMainId>0) {
          previousMainId--;
        }
      }
      if(previousMainId==0) {
        previousMainId=1;
      }
    
      // comment out the following to fix bug 31789
//       if (previousMainId > 1) {
//         previousMainId--;
//       }
      File result = null;
        int childId = calcNextChildId(log, previousMainId > 0 ? previousMainId : 0);
        StringBuffer buf = new StringBuffer(log.getPath());
        int insertIdx = buf.lastIndexOf(".");
        if (insertIdx == -1) {
          buf
            .append(formatId(previousMainId))
            .append(formatId(childId));
        } else {
          buf.insert(insertIdx, formatId(childId));
          buf.insert(insertIdx, formatId(previousMainId));
        }
        result = new File(buf.toString());
      return result;
    }
    

    private static Pattern getLogPattern(String name) {
      int extIdx = name.lastIndexOf('.');
      String ext = "";
      if (extIdx != -1) {
        ext = "\\Q" + name.substring(extIdx) + "\\E";
        name = name.substring(0, extIdx);
      }
      name = "\\Q" + name + "\\E" + "-\\d+-\\d+" + ext;
      return Pattern.compile(name);
    }

    protected static final Pattern mainIdPattern = Pattern.compile(".+-\\d+-\\d+\\..+");
    protected static final Pattern metaIdPattern = Pattern.compile("meta-.+-\\d+\\..+");

    public static int calcNextMainId(File dir, boolean toCreateNew) {
      int result = 0;
      File[] childLogs = FileUtil.listFiles(dir, new FilenameFilter() {
          public boolean accept(File d, String name) {
            return mainIdPattern.matcher(name).matches();
          }
        });
      
      /* Search child logs */
      for (File childLog: childLogs) {
        String name = childLog.getName();
        int endIdIdx = name.lastIndexOf('-');
        int startIdIdx = name.lastIndexOf('-', endIdIdx-1);
        String id = name.substring(startIdIdx+1, endIdIdx);
        try {
          int mid= Integer.parseInt(id);
          if (mid > result) {
            result = mid;
          }
        } catch (NumberFormatException ignore) {
        }
      }
      
      /* And search meta logs */
      if (toCreateNew) {
        File[] metaLogs = FileUtil.listFiles(dir, new FilenameFilter() {
          public boolean accept(File d, String name) {
            return metaIdPattern.matcher(name).matches();
          }
        });
        for (File metaLog: metaLogs) {
          String name = metaLog.getName();
          int endIdIdx = name.lastIndexOf('.');
          int startIdIdx = name.lastIndexOf('-', endIdIdx-1);
          String id = name.substring(startIdIdx+1, endIdIdx);
          try {
            int mid = Integer.parseInt(id);
            if (mid > result) {
              result = mid;
            }
          } catch (NumberFormatException ignore) {
          }
        }
        result++;
      }

      return result;
    }
    protected static final Pattern childIdPattern = Pattern.compile(".+-\\d+-\\d+\\..+");

    
    public static int calcNextChildId(File log,int mainId) {
      int result = 0;
      File dir = getParentFile(log.getAbsoluteFile());
      int endidx1 = log.getName().indexOf('-');
      int endidx2 = log.getName().lastIndexOf('.');
      String baseName = log.getName();
      if (endidx1 != -1) {
        baseName =  log.getName().substring(0, endidx1);
      } else {
        baseName =  log.getName().substring(0, endidx2);
      }
      File[] childLogs = FileUtil.listFiles(dir, new FilenameFilter() {
          public boolean accept(File d, String name) {
            return childIdPattern.matcher(name).matches();
          }
        });
      
      /* Search child logs */
      
      for (File childLog: childLogs) {
        String name = childLog.getName();
        // only compare the childlogid among the same set of log files.
        if (!name.startsWith(baseName)) {
          continue;
        }
        int endIdIdx = name.lastIndexOf('-');
        int startIdIdx = name.lastIndexOf('-', endIdIdx-1);
        String id = name.substring(startIdIdx+1, endIdIdx);
        
        int startChild = name.lastIndexOf("-");
        int endChild = name.lastIndexOf(".");
        if(startChild>0 && endChild>0) {
          String childId = name.substring(startChild+1,endChild);
          
          try {
            int mainLogId = Integer.parseInt(id);
            int childLogId = Integer.parseInt(childId);
            if (mainLogId ==mainId && childLogId>result) {
              result = childLogId;
            }
          } catch (NumberFormatException ignore) {
          }
        }
      }
      result++;
      return result;
    }
    
    

//  private static void debugLog(String msg, boolean stackDump) {
//    try {
//      FileOutputStream f = new FileOutputStream("debug.log", true);
//      LogWriterI18n lw = new LocalLogWriter(ALL_LEVEL, new PrintStream(f));
//      if (stackDump) {
//        lw.info(msg, new RuntimeException("STACK"));
//      } else {
//        lw.info(msg);
//      }
//      f.close();
//    } catch (IOException ignore) {
//    }
//  }
  
    public static void removeOldLogs(LogConfig cfg,
                                     File logFile) {
      LogWriterI18n log = new LocalLogWriter(INFO_LEVEL, System.err);
      checkDiskSpace("log", null,
                     ((long)cfg.getLogDiskSpaceLimit()) * (1024*1024),
                     getParentFile(logFile),
                     getLogPattern(logFile.getName()),
                     log);
    }
  
    public static void checkDiskSpace(String type,
                                       File newLog,
                                       long spaceLimit,
                                       File dir,
                                       final Pattern logPattern,
                                       LogWriterI18n logger) {
      if (spaceLimit == 0 || logPattern == null) {
        return;
      }
      final String newLogName = (newLog == null) ? null : newLog.getName();
      File[] childLogs = FileUtil.listFiles(dir, new FilenameFilter() {
          public boolean accept(File d, String name) {
            if (name.equals(newLogName)) {
              return false;
            } else {
              boolean result = logPattern.matcher(name).matches();
              return result;
            }
          }
        });
      if (childLogs == null) {
        if (dir.isDirectory()) {
          logger.warning(
              LocalizedStrings.ManagerLogWriter_COULD_NOT_CHECK_DISK_SPACE_ON_0_BECAUSE_JAVAIOFILELISTFILES_RETURNED_NULL_THIS_COULD_BE_CAUSED_BY_A_LACK_OF_FILE_DESCRIPTORS,
              dir);
        }
        return;
      }
      Arrays.sort(childLogs, new Comparator() {
          public int compare(Object o1, Object o2) {
            File f1 = (File)o1;
            File f2 = (File)o2;
            long diff =  f1.lastModified() - f2.lastModified();
            if (diff < 0) {
              return -1;
            } else if (diff > 0) {
              return 1;
            } else {
              return 0;
            }
          }
        });
      long spaceUsed = 0;
      for (File childLog: childLogs) {
        spaceUsed += childLog.length();
      }
      int fIdx = 0;
      while (spaceUsed >= spaceLimit
             && fIdx < childLogs.length) { // check array index to 37388
        long childSize = childLogs[fIdx].length();
        if (childLogs[fIdx].delete()) {
            spaceUsed -= childSize;
          logger.info(LocalizedStrings.ManagerLogWriter_DELETED_INACTIVE__0___1_, new Object[] {type, childLogs[fIdx]});
        } else {
          logger.warning(LocalizedStrings.ManagerLogWriter_COULD_NOT_DELETE_INACTIVE__0___1_, new Object[] {type, childLogs[fIdx]});
        }
        fIdx++;
      }
      if (spaceUsed > spaceLimit) {
        logger.warning(
            LocalizedStrings.ManagerLogWriter_COULD_NOT_FREE_SPACE_IN_0_DIRECTORY_THE_SPACE_USED_IS_1_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_2,
            new Object[] {type, Long.valueOf(spaceUsed), Long.valueOf(spaceLimit)});
      }
    }

    private void checkDiskSpace(File newLog) {
      checkDiskSpace("log", newLog, getLogDiskSpaceLimit(), logDir, childLogPattern, mainLogger);
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

  private boolean started = false;
    /**
     * Called when manager is done starting up.
     * This is when a child log will be started if rolling is configured.
     */
    public void startupComplete() {
      started = true;
      rollLog();
    }
    /**
     * Called when manager starts shutting down.
     * This is when any current child log needs to be closed and the
     * rest of the logging reverted to the main.
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
    public String put(int msgLevel, Date msgDate,
      String connectionName, String threadName, long tid, String msg,
      String exceptionText) {

      String result = null; // This seems to workaround a javac bug
      result = super.put(msgLevel, msgDate, connectionName, threadName,
        tid, msg, exceptionText);
      return result;
    }

    @Override
    public void writeFormattedMessage(String s) {
      rollLogIfFull();
      super.writeFormattedMessage(s);
    }
}
