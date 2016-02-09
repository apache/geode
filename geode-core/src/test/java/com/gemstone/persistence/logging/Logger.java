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
package com.gemstone.persistence.logging;

import java.util.*;

/**
 * A logger is used to record messages and events.  Each entry has a
 * given level associated with it.  There are a number of convenience
 * methods for logging events.  Events with a level above a certain
 * are written to <code>System.err</code>.  The default level is
 * INFO
 */
public class Logger {
  private static boolean DEBUG = Boolean.getBoolean("Logging.DEBUG");

  /** Maps the names of Loggers to the Logger */
  private static Map loggers = new HashMap();

  /** The name of this logger */
  private String name;

  /** The maximum level at which messages are logged.  Message level
   * lower than this value will be ignored. */
  private Level level;

  /** The Handlers to which this logger's records are sent */
  private Set handlers;

  /**
   * Creates a new <code>Logger</code> with the given name
   */
  protected Logger(String name) {
    this.name = name;

    // Uses a system property to set the level
    String prop = System.getProperty(name + ".LEVEL");
    if(prop != null) {
      this.level = Level.parse(prop);
    } else {
      this.level = Level.INFO;
    }

    this.handlers = new HashSet();

    // By default, log to System.err
    this.handlers.add(new StreamHandler(System.err, 
                                        new SimpleFormatter()));
  }

  /**
   * Returns the logger with the given name
   */
  public synchronized static Logger getLogger(String name) {
    Logger logger = (Logger) loggers.get(name);
    if(logger == null) {
      logger = new Logger(name);
      loggers.put(name, logger);
    }

//    Assert.assertTrue(logger != null); (cannot be null)
    return(logger);
  }

  /**
   * Adds a Handler to receive logging messages
   */
  public synchronized void addHandler(Handler handler) {
    this.handlers.add(handler);
  }

  /**
   * Returns the Handlers associated with this logger
   */
  public synchronized Handler[] getHandlers() {
    return((Handler[]) this.handlers.toArray(new Handler[0]));
  }

  /**
   * Removes a Handler from this logger
   */
  public synchronized void removeHandler(Handler handler) {
    this.handlers.remove(handler);
  }

  /**
   * Returns the log level specifying which messages will be logged by
   * this logger.
   */
  public synchronized Level getLevel() {
    return(this.level);
  }

  /**
   * Sets the log level specifying which messages will be logged by
   * this logger.
   */
  public synchronized void setLevel(Level level) {
    this.level = level;
  }

  /**
   * Check if a message of the given level would actually be logged by
   * this logger.
   */
  public synchronized boolean isLoggable(Level msgLevel) {
    if(msgLevel.equals(Level.ALL)) {
      // Always log Level.ALL messages. Is this a logic error?
      return(true);
    } else {
      return(msgLevel.intValue() >= this.level.intValue());
    }
  }

  /**
   * Prints the given log record to System.err
   */
  public synchronized void log(LogRecord record) {
    if(!isLoggable(record.getLevel())) {
      // This record is beneath us
      return;
    }

    if(DEBUG) {
      System.out.println("Logging " + record);
    }

    // Publish the record to each handler
    Iterator iter = this.handlers.iterator();
    while(iter.hasNext()) {
      Handler handler = (Handler) iter.next();
      handler.publish(record);
      handler.flush();
    }
  }

  /**
   * Logs a CONFIG message
   */
  public synchronized void config(String msg) {
    LogRecord record = new LogRecord(Level.CONFIG, msg);
    log(record);
  }

  /**
   * Log a CONFIG message, with an array of object arguments.
   */
  public synchronized void config(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.CONFIG, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a CONFIG message, specifying source class and method.
   */
  public synchronized void config(String sourceClass, 
                                  String sourceMethod, String msg) {
    LogRecord record = new LogRecord(Level.CONFIG, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a CONFIG message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void config(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.CONFIG, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a procedure entry.
   */
  public synchronized void entering(String sourceClass, String sourceMethod) {
    LogRecord record = new LogRecord(Level.CONFIG, "Entering method");
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a procedure entry, with parameters.
   */
  public synchronized void entering(String sourceClass, String sourceMethod,
                       Object[] params) {
    LogRecord record = new LogRecord(Level.CONFIG, "Entering method");
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a procedure return.
   */
  public synchronized void exiting(String sourceClass, String sourceMethod) {
    LogRecord record = new LogRecord(Level.CONFIG, "Exiting method");
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a procedure return, with parameters.
   */
  public synchronized void exiting(String sourceClass, String sourceMethod,
                       Object[] params) {
    LogRecord record = new LogRecord(Level.CONFIG, "Exiting method");
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }
  
  /**
   * Logs a FINE message
   */
  public synchronized void fine(String msg) {
    LogRecord record = new LogRecord(Level.FINE, msg);
    log(record);
  }

  /**
   * Log a FINE message, with an array of object arguments.
   */
  public synchronized void fine(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINE, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a FINE message, specifying source class and method.
   */
  public synchronized void fine(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.FINE, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a FINE message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void fine(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINE, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Logs a FINER message
   */
  public synchronized void finer(String msg) {
    LogRecord record = new LogRecord(Level.FINER, msg);
    log(record);
  }

  /**
   * Log a FINER message, with an array of object arguments.
   */
  public synchronized void finer(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINER, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a FINER message, specifying source class and method.
   */
  public synchronized void finer(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.FINER, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a FINER message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void finer(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINER, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Logs a FINEST message
   */
  public synchronized void finest(String msg) {
    LogRecord record = new LogRecord(Level.FINEST, msg);
    log(record);
  }

  /**
   * Log a FINEST message, with an array of object arguments.
   */
  public synchronized void finest(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINEST, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a FINEST message, specifying source class and method.
   */
  public synchronized void finest(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.FINEST, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a FINEST message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void finest(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.FINEST, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Logs a INFO message
   */
  public synchronized void info(String msg) {
    LogRecord record = new LogRecord(Level.INFO, msg);
    log(record);
  }

  /**
   * Log a INFO message, with an array of object arguments.
   */
  public synchronized void info(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.INFO, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a INFO message, specifying source class and method.
   */
  public synchronized void info(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.INFO, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a INFO message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void info(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.INFO, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Logs a message
   */
  public synchronized void log(Level msgLevel, String msg) {
    LogRecord record = new LogRecord(msgLevel, msg);
    log(record);
  }

  /**
   * Log a  message, with an array of object arguments.
   */
  public synchronized void log(Level msgLevel, String msg, Object[] params) {
    LogRecord record = new LogRecord(msgLevel, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a  message, specifying source class and method.
   */
  public synchronized void log(Level msgLevel, String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(msgLevel, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a  message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void log(Level msgLevel, String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(msgLevel, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a message, specifying source class and method, with
   * associated Throwable information.
   */
  public synchronized void log(Level msgLevel, String sourceClass,
                  String sourceMethod, String msg, Throwable thrown) {
    LogRecord record = new LogRecord(msgLevel, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setThrown(thrown);
    log(record);
  }

  /**
   * Log a message, with associated Throwable information.
   */
  public synchronized void log(Level msgLevel, String msg, Throwable thrown) {
    LogRecord record = new LogRecord(msgLevel, msg);
    record.setThrown(thrown);
    log(record);
  }

  /**
   * Logs a SEVERE message
   */
  public synchronized void severe(String msg) {
    LogRecord record = new LogRecord(Level.SEVERE, msg);
    log(record);
  }

  /**
   * Log a SEVERE message, with an array of object arguments.
   */
  public synchronized void severe(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.SEVERE, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a SEVERE message, specifying source class and method.
   */
  public synchronized void severe(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.SEVERE, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a SEVERE message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void severe(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.SEVERE, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log throwing an exception.  The logging is done using the FINER
   * level. 
   */
  public synchronized void throwing(String sourceClass, String sourceMethod,
                       Throwable thrown) {
    LogRecord record = new LogRecord(Level.FINER, "THROWN");
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setThrown(thrown);
    log(record);
  }

  /**
   * Logs a WARNING message
   */
  public synchronized void warning(String msg) {
    LogRecord record = new LogRecord(Level.WARNING, msg);
    log(record);
  }

  /**
   * Log a WARNING message, with an array of object arguments.
   */
  public synchronized void warning(String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.WARNING, msg);
    record.setParameters(params);
    log(record);
  }

  /**
   * Log a WARNING message, specifying source class and method.
   */
  public synchronized void warning(String sourceClass, String sourceMethod, 
                     String msg) {
    LogRecord record = new LogRecord(Level.WARNING, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    log(record);
  }

  /**
   * Log a WARNING message, specifying source class and method, with an
   * array of object arguments.
   */
  public synchronized void warning(String sourceClass, String sourceMethod,
                     String msg, Object[] params) {
    LogRecord record = new LogRecord(Level.WARNING, msg);
    record.setSourceClassName(sourceClass);
    record.setSourceMethodName(sourceMethod);
    record.setParameters(params);
    log(record);
  }

  /**
   * Formats a message.  Takes special care when invoking the
   * toString() method of objects that might cause NPEs.
   */
  public static String format(String format, Object[] objs) {
    return com.gemstone.persistence.admin.Logger.format( format, objs );
  }

}
