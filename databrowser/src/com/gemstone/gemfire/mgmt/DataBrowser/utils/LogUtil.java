/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.utils;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;

public class LogUtil {
//  static final String    ENV_FILE_SYS_SEPARATOR = System
//                                                    .getProperty("file.separator");
//  static final String    ENV_JAVA_LOGDIR        = System.getProperty("log.dir");
//  static final String    ENV_USER_HOME          = System
//                                                    .getProperty("user.home");
//  static final String    ENV_CWD                = System
//                                                    .getProperty("user.dir");
//  static final String    ENV_JAVA_TMPDIR        = System
//                                                    .getProperty("java.io.tmpdir");

  /*
   * Global logger exists that can be accessed using Logger.getLogger("").
   */
  private static Logger  logger                 = Logger.getLogger("com.gemstone.gemfire.mgmt.DataBrowser");
  private static boolean DEBUG                  = Boolean.getBoolean("debug");

  public static final Level LOG_LEVEL_ALL = Level.ALL;
  public static final Level LOG_LEVEL_FINEST = Level.FINEST;
  public static final Level LOG_LEVEL_FINER = Level.FINER;
  public static final Level LOG_LEVEL_FINE = Level.FINE;
  public static final Level LOG_LEVEL_CONFIG = Level.CONFIG;
  public static final Level LOG_LEVEL_INFO = Level.INFO;
  public static final Level LOG_LEVEL_WARNING = Level.WARNING;
  public static final Level LOG_LEVEL_SEVERE = Level.SEVERE;
  public static final Level LOG_LEVEL_NONE = Level.OFF;

  private static final Set<String> logLevelStr;

  static {
    //don't pass logs to parent global logger handlers.
    logger.setUseParentHandlers(false);
    Formatter formatter = new ToolsFormater();
    try {
      Handler[] handlers = logger.getHandlers();
      for (int i = 0; i < handlers.length; i++) {
        logger.removeHandler(handlers[i]);
      }

      if (DEBUG) {
        addConsoleHandler(formatter);
      } else {
        String logDir = getLogDir();
        boolean result = prepareLogDir(logDir);
        if (result) {
          //TODO localize the file name
          int logFileSize = DataBrowserPreferences.getLogFileSize() * 1024 * 1024; //Convert from MB to bytes.
          int logFileCount = DataBrowserPreferences.getLogFileCount();
          FileHandler handler = new FileHandler(logDir + "/data-browser_" + getProcessId() + ".log", logFileSize,logFileCount);
          handler.setFormatter(formatter);

          logger.addHandler(handler);
          setLogLevel();
        } else {
          addConsoleHandler(formatter);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      addConsoleHandler(formatter);
    }
    logLevelStr = new LinkedHashSet<String>();
    logLevelStr.add(LOG_LEVEL_ALL.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_FINEST.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_FINER.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_FINE.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_CONFIG.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_INFO.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_WARNING.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_SEVERE.getLocalizedName());
    logLevelStr.add(LOG_LEVEL_NONE.getLocalizedName());
  }


  public static String [] getLoglevelStrings(){
    return logLevelStr.toArray(new String[0]);
  }

  private static void setLogLevel(){
    String loggingString = DataBrowserPreferences.getLoggingLevel();
    Level level = Level.parse(loggingString);
    logger.setLevel(level);
  }

  public static void error(String msg) {
    logger.log(Level.SEVERE, msg);
  }

  public static void error(String msg, Throwable ex) {
    logger.log(Level.SEVERE, msg, ex);
  }

  public static void warning(String msg) {
    logger.log(Level.WARNING, msg);
  }

  public static void warning(String msg, Throwable ex) {
    logger.log(Level.WARNING, msg, ex);
  }

  public static void info(String msg, Throwable ex) {
    logger.log(Level.INFO, msg, ex);
  }

  public static void info(String msg) {
    logger.log(Level.INFO, msg);
  }

  public static void fine(String msg, Throwable ex) {
    logger.log(Level.FINE, msg, ex);
  }

  public static void fine(String msg) {
    logger.log(Level.FINE, msg);
  }
  
  public static void finest(String msg) {
    logger.log(Level.FINEST, msg);
  }
  

  // MGH - Changes this to have consistent behavior across products
  // viz. if log.dir not specified, use a folder
  // (Gemstone/GemFire/DataBrowser/logs)
  // under the user's home directory, or then the current working directory, or
  // then
  // the temp directory used by Java, else whatever
  // "Gemstone/GemFire/DataBrowser/logs"
  // resolves! (Does the last option make sense?)
  public static String getLogDir() {
    return DataBrowserPreferences.getLogDirectory();
  }

  private static boolean prepareLogDir(String logDir) {
    File dir = new File(logDir);

    if (!dir.exists()) {
      return dir.mkdirs();
    }

    return true;
  }

  // TODO MGH - Trac this as a bug. Using the console is file while development, but
  // not necessarily acceptable for production.
  // Identify what could cause the logger to not initialize
  private static void addConsoleHandler(Formatter formatter) {
    if (!DEBUG)
      System.out
          .println("Could not initialize the Logger. Using ConsoleHandler...");
    ConsoleHandler handler = new ConsoleHandler();
    handler.setFormatter(formatter);
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    setLogLevel();
  }

  public static String getProcessId() {
    // TODO : This is a quick hack to fix the Log file issues.
    String name = ManagementFactory.getRuntimeMXBean().getName();
    int index = name.indexOf('@');
    if (index != -1)
      name = name.substring(0, index);
    return name;
  }
}
