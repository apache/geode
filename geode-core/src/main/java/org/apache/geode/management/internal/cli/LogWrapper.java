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
package org.apache.geode.management.internal.cli;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.BreakIterator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.shell.GfshConfig;

/**
 * NOTE: Should be used only in 1. gfsh process 2. on a Manager "if" log is required to be sent back
 * to gfsh too. For logging only on manager use, cache.getLogger()
 *
 * @since GemFire 7.0
 */
public class LogWrapper {
  private static Object INSTANCE_LOCK = new Object();
  private static volatile LogWrapper INSTANCE = null;

  private Logger logger;

  private LogWrapper(Cache cache) {
    logger = Logger.getLogger(this.getClass().getCanonicalName());

    if (cache != null && !cache.isClosed()) {
      logger.addHandler(cache.getLogger().getHandler());
      CommandResponseWriterHandler handler = new CommandResponseWriterHandler();
      handler.setFilter(record -> record.getLevel().intValue() >= Level.FINE.intValue());
      handler.setLevel(Level.FINE);
      logger.addHandler(handler);
    }
    logger.setUseParentHandlers(false);
  }

  public static LogWrapper getInstance(Cache cache) {
    if (INSTANCE == null) {
      synchronized (INSTANCE_LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new LogWrapper(cache);
        }
      }
    }

    return INSTANCE;
  }

  public void configure(GfshConfig config) {
    if (config.isLoggingEnabled()) {
      try {
        FileHandler fileHandler = new FileHandler(config.getLogFilePath(),
            config.getLogFileSizeLimit(), config.getLogFileCount(), true);
        fileHandler.setFormatter(new GemFireFormatter());
        fileHandler.setLevel(config.getLogLevel());
        logger.addHandler(fileHandler);
        logger.setLevel(config.getLogLevel());
      } catch (SecurityException | IOException e) {
        addDefaultConsoleHandler(logger, e.getMessage(), config.getLogFilePath());
      }
    }
  }

  private static void addDefaultConsoleHandler(Logger logger, String errorMessage,
      String logFilePath) {
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setFormatter(new GemFireFormatter());
    logger.addHandler(consoleHandler);

    System.err
        .println("ERROR: Could not log to file: " + logFilePath + ". Reason: " + errorMessage);
    System.err.println("Logs will be written on Console.");
    try {
      Thread.sleep(3000); // sleep for 3 secs for the message to appear
    } catch (InterruptedException ignore) {
    }
  }

  /**
   * Closes the current LogWrapper.
   */
  public static void close() {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE != null) {
        Logger innerLogger = INSTANCE.logger;
        // remove any existing handlers
        cleanupLogger(innerLogger);
      }
      // make singleton null
      INSTANCE = null;
    }
  }

  /**
   * Removed all the handlers of the given {@link Logger} instance.
   *
   * @param logger {@link Logger} to be cleaned up.
   */
  private static void cleanupLogger(Logger logger) {
    if (logger != null) {
      Handler[] handlers = logger.getHandlers();
      for (Handler handler : handlers) {
        handler.close();
        logger.removeHandler(handler);
      }
    }
  }

  /**
   * Make logger null when the singleton (which was referred by INSTANCE) gets garbage collected.
   * Makes an attempt at removing associated {@link Handler}s of the {@link Logger}.
   */
  protected void finalize() throws Throwable {
    cleanupLogger(this.logger);
    this.logger = null;
  }

  public void setParentFor(Logger otherLogger) {
    if (otherLogger.getParent() != logger) {
      otherLogger.setParent(logger);
    }
  }

  public void setLogLevel(Level newLevel) {
    if (logger.getLevel() != newLevel) {
      logger.setLevel(newLevel);
    }
  }

  public Level getLogLevel() {
    return logger.getLevel();
  }

  Logger getLogger() {
    return logger;
  }

  public boolean severeEnabled() {
    return logger.isLoggable(Level.SEVERE);
  }

  public void severe(String message) {
    if (severeEnabled()) {
      logger.severe(message);
    }
  }

  public void severe(String message, Throwable t) {
    if (severeEnabled()) {
      logger.log(Level.SEVERE, message, t);
    }
  }

  public boolean warningEnabled() {
    return logger.isLoggable(Level.WARNING);
  }

  public void warning(String message) {
    if (warningEnabled()) {
      logger.warning(message);
    }
  }

  public void warning(String message, Throwable t) {
    if (warningEnabled()) {
      logger.log(Level.WARNING, message, t);
    }
  }

  public boolean infoEnabled() {
    return logger.isLoggable(Level.INFO);
  }

  public void info(String message) {
    if (infoEnabled()) {
      logger.info(message);
    }
  }

  public void info(String message, Throwable t) {
    if (infoEnabled()) {
      logger.log(Level.INFO, message, t);
    }
  }

  public boolean configEnabled() {
    return logger.isLoggable(Level.CONFIG);
  }

  public void config(String message) {
    if (configEnabled()) {
      logger.config(message);
    }
  }

  public void config(String message, Throwable t) {
    if (configEnabled()) {
      logger.log(Level.CONFIG, message, t);
    }
  }

  public boolean fineEnabled() {
    return logger.isLoggable(Level.FINE);
  }

  public void fine(String message) {
    if (fineEnabled()) {
      logger.fine(message);
    }
  }

  public void fine(String message, Throwable t) {
    if (fineEnabled()) {
      logger.log(Level.FINE, message, t);
    }
  }

  public boolean finerEnabled() {
    return logger.isLoggable(Level.FINER);
  }

  public void finer(String message) {
    if (finerEnabled()) {
      logger.finer(message);
    }
  }

  public void finer(String message, Throwable t) {
    if (finerEnabled()) {
      logger.log(Level.FINER, message, t);
    }
  }

  public boolean finestEnabled() {
    return logger.isLoggable(Level.FINEST);
  }

  public void finest(String message) {
    if (finestEnabled()) {
      logger.finest(message);
    }
  }

  public void finest(String message, Throwable t) {
    if (finestEnabled()) {
      logger.log(Level.FINEST, message, t);
    }
  }

  /**
   *
   * @since GemFire 7.0
   */
  static class GemFireFormatter extends Formatter {
    private static final String FORMAT = "yyyy/MM/dd HH:mm:ss.SSS z";

    private SimpleDateFormat sdf = new SimpleDateFormat(FORMAT);

    @Override
    public synchronized String format(LogRecord record) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);

      pw.println();
      pw.print('[');
      pw.print(record.getLevel().getName().toLowerCase());
      pw.print(' ');
      pw.print(formatDate(new Date(record.getMillis())));
      String threadName = Thread.currentThread().getName();
      if (threadName != null) {
        pw.print(' ');
        pw.print(threadName);
      }
      pw.print(" tid=0x");
      pw.print(Long.toHexString(Thread.currentThread().getId()));
      pw.print("] ");
      pw.print("(msgTID=");
      pw.print(record.getThreadID());

      pw.print(" msgSN=");
      pw.print(record.getSequenceNumber());
      pw.print(") ");

      String msg = record.getMessage();
      if (msg != null) {
        try {
          formatText(pw, msg, 40);
        } catch (RuntimeException e) {
          pw.println(msg);
          pw.println("Ignoring the following exception:");
          e.printStackTrace(pw);
        }
      } else {
        pw.println();
      }
      if (record.getThrown() != null) {
        record.getThrown().printStackTrace(pw);
      }
      pw.close();
      try {
        sw.close();
      } catch (IOException ignore) {
      }
      return sw.toString();
    }

    private void formatText(PrintWriter writer, String target, int initialLength) {
      BreakIterator boundary = BreakIterator.getLineInstance();
      boundary.setText(target);
      int start = boundary.first();
      int end = boundary.next();
      int lineLength = initialLength;

      while (end != BreakIterator.DONE) {
        // Look at the end and only accept whitespace breaks
        char endChar = target.charAt(end - 1);
        while (!Character.isWhitespace(endChar)) {
          int lastEnd = end;
          end = boundary.next();
          if (end == BreakIterator.DONE) {
            // give up. We are at the end of the string
            end = lastEnd;
            break;
          }
          endChar = target.charAt(end - 1);
        }
        int wordEnd = end;
        if (endChar == '\n') {
          // trim off the \n since println will do it for us
          wordEnd--;
          if (wordEnd > 0 && target.charAt(wordEnd - 1) == '\r') {
            wordEnd--;
          }
        } else if (endChar == '\t') {
          // figure tabs use 8 characters
          lineLength += 7;
        }
        String word = target.substring(start, wordEnd);
        lineLength += word.length();
        writer.print(word);
        if (endChar == '\n' || endChar == '\r') {
          // force end of line
          writer.println();
          writer.print("  ");
          lineLength = 2;
        }
        start = end;
        end = boundary.next();
      }
      if (lineLength != 0) {
        writer.println();
      }
    }

    private String formatDate(Date date) {
      return sdf.format(date);
    }
  }

  /**
   * Handler to write to CommandResponseWriter
   *
   * @since GemFire 7.0
   */
  static class CommandResponseWriterHandler extends Handler {

    public CommandResponseWriterHandler() {
      setFormatter(new GemFireFormatter());
    }

    @Override
    public void publish(LogRecord record) {
      CommandResponseWriter responseWriter =
          CommandExecutionContext.getAndCreateIfAbsentCommandResponseWriter();
      responseWriter.println(getFormatter().format(record));
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}
  }
}
