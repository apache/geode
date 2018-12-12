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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.apache.geode.GemFireIOException;
import org.apache.geode.i18n.StringId;

/**
 * Implementation of {@link org.apache.geode.LogWriter} that will write to a local stream
 * and only use pure java features.
 */
public class PureLogWriter extends LogWriterImpl {

  /** The "name" of the connection associated with this log writer */
  private final String connectionName;

  /** If the log stream has been closed */
  private volatile boolean closed;

  protected volatile int level;

  private PrintWriter printWriter;
  private long bytesLogged;

  /**
   * Creates a writer that logs to <code>System.out</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public PureLogWriter(final int level) {
    this(level, System.out);
  }

  /**
   * Creates a writer that logs to <code>printStream</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param printStream is the stream that message will be printed to.
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public PureLogWriter(final int level, final PrintStream printStream) {
    this(level, new PrintWriter(printStream, true), null);
  }

  /**
   * Creates a writer that logs to <code>printStream</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param printStream is the stream that message will be printed to.
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public PureLogWriter(final int level, final PrintStream printStream,
      final String connectionName) {
    this(level, new PrintWriter(printStream, true), connectionName);
  }

  /**
   * Creates a writer that logs to <code>printWriter</code>.
   *
   * @param level only messages greater than or equal to this value will be logged.
   * @param printWriter is the stream that message will be printed to.
   * @param connectionName The name of the connection associated with this log writer
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public PureLogWriter(final int level, final PrintWriter printWriter,
      final String connectionName) {
    setLevel(level);
    this.printWriter = printWriter;
    this.connectionName = connectionName;
  }

  /**
   * Gets the writer's level.
   */
  @Override
  public int getLogWriterLevel() {
    return level;
  }

  /**
   * Sets the writer's level.
   *
   * @throws IllegalArgumentException if level is not in legal range
   */
  public void setLevel(final int level) {
    this.level = level;
  }

  @Override
  public void setLogWriterLevel(final int logWriterLevel) {
    setLevel(logWriterLevel);
  }

  protected String getThreadName() {
    return Thread.currentThread().getName();
  }

  protected long getThreadId() {
    // fix for bug 37861
    return Thread.currentThread().getId();
  }

  /**
   * Logs a message and an exception to the specified log destination.
   *
   * @param messageLevel a string representation of the level
   * @param message the actual message to log
   * @param throwable the actual Exception to log
   */
  @Override
  public void put(final int messageLevel, final String message, final Throwable throwable) {
    String exceptionText = null;
    if (throwable != null) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      throwable.printStackTrace(printWriter);
      printWriter.close();
      try {
        stringWriter.close();
      } catch (IOException ignore) {
      }
      exceptionText = stringWriter.toString();
    }
    put(messageLevel, new Date(), connectionName, getThreadName(), getThreadId(), message,
        exceptionText);
  }

  /**
   * Logs a message and an exception to the specified log destination.
   *
   * @param messageLevel a string representation of the level
   * @param messageId the actual message to log
   * @param throwable the actual Exception to log
   */
  @Override
  public void put(final int messageLevel, final StringId messageId, final Object[] parameters,
      final Throwable throwable) {
    String message = messageId.toLocalizedString(parameters);
    put(messageLevel, message, throwable);
  }

  private String formatLogLine(final int messageLevel, final Date messageDate,
      final String connectionName, final String threadName, final long threadId,
      final String message, final String exceptionText) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);

    printHeader(printWriter, messageLevel, messageDate, connectionName, threadName, threadId);

    if (message != null) {
      try {
        formatText(printWriter, message, 40);
      } catch (RuntimeException e) {
        printWriter.println(message);
        printWriter.println("Ignoring exception: ");
        e.printStackTrace(printWriter);
      }
    } else {
      printWriter.println();
    }
    if (exceptionText != null) {
      printWriter.print(exceptionText);
    }
    printWriter.close();
    try {
      stringWriter.close();
    } catch (IOException ignore) {
      // ignored
    }

    return stringWriter.toString();
  }

  private void printHeader(final PrintWriter printWriter, final int messageLevel,
      final Date messageDate, final String connectionName, final String threadName,
      final long threadId) {
    printWriter.println();
    printWriter.print('[');
    printWriter.print(levelToString(messageLevel));
    printWriter.print(' ');
    printWriter.print(formatDate(messageDate));
    if (connectionName != null) {
      printWriter.print(' ');
      printWriter.print(connectionName);
    }
    if (threadName != null) {
      printWriter.print(" <");
      printWriter.print(threadName);
      printWriter.print(">");
    }
    printWriter.print(" tid=0x");
    printWriter.print(Long.toHexString(threadId));
    printWriter.print("] ");
  }

  public String put(final int messageLevel, final Date messageDate, final String connectionName,
      final String threadName, final long threadId, final String message,
      final String exceptionText) {
    String formattedLine = formatLogLine(messageLevel, messageDate, connectionName, threadName,
        threadId, message, exceptionText);
    writeFormattedMessage(formattedLine);
    return formattedLine;
  }

  public void writeFormattedMessage(final String message) {
    synchronized (this) {
      bytesLogged += message.length();
      printWriter.print(message);
      printWriter.flush();
    }
  }

  /**
   * Returns the number of bytes written to the current log file.
   */
  long getBytesLogged() {
    return bytesLogged;
  }

  /**
   * Sets the target that this logger will sends its output to.
   *
   * @return the previous target.
   */
  public PrintWriter setTarget(final PrintWriter printWriter) {
    return setTarget(printWriter, 0L);
  }

  public PrintWriter setTarget(final File logFile) {
    return setTarget(createFileOutputStream(logFile), 0L);
  }

  private PrintWriter createFileOutputStream(final File logFile) {
    try {
      return new PrintWriter(new FileOutputStream(logFile, true), true);
    } catch (FileNotFoundException ex) {
      String s = String.format("Could not open log file \"%s\".", logFile);
      throw new GemFireIOException(s, ex);
    }
  }

  public PrintWriter setTarget(final PrintWriter printWriter, final long targetLength) {
    synchronized (this) {
      PrintWriter oldPrintWriter = this.printWriter;
      bytesLogged = targetLength;
      this.printWriter = printWriter;
      return oldPrintWriter;
    }
  }

  public void close() {
    closed = true;
    try {
      if (printWriter != null) {
        printWriter.close();
      }
    } catch (Exception e) {
      // ignore , we are closing.
    }
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * Returns the name of the connection on whose behalf this log writer logs.
   */
  @Override
  public String getConnectionName() {
    return connectionName;
  }
}
