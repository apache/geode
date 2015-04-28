/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.logging;

import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;

/**
 * Implementation of the standard JDK handler that publishes a log record
 * to a LogWriterImpl.
 * Note this handler ignores any installed handler.
 */
public final class GemFireHandler extends Handler {

  /**
   * Use the log writer to use some of its formatting code.
   */
  private LogWriter logWriter;

  public GemFireHandler(LogWriter logWriter) {
    this.logWriter = logWriter;
    this.setFormatter(new GemFireFormatter(logWriter));
  }

  @Override
  public void close() {
    // clear the reference to GFE LogWriter
    this.logWriter = null;
  }

  @Override
  public void flush() {
    // nothing needed
  }

  private String getMessage(LogRecord record) {
    final StringBuilder b = new StringBuilder();
    b .append('(')
      .append("tid=" + record.getThreadID())
      .append(" msgId=" + record.getSequenceNumber())
      .append(") ");
    if (record.getMessage() != null) {
      b.append(getFormatter().formatMessage(record));
    }
    return b.toString();
  }

  @Override
  public void publish(LogRecord record) {
    if (isLoggable(record)) {
      try {
        if (this.logWriter instanceof LogWriterLogger) {
          ((LogWriterLogger) this.logWriter).log(record.getLevel().intValue(), getMessage(record), record.getThrown());
        } else {
          ((LogWriterImpl) this.logWriter).put(record.getLevel().intValue(), getMessage(record), record.getThrown());
        }
      } catch (GemFireException ex) {
        reportError(null, ex, ErrorManager.WRITE_FAILURE);
      }
    }
  }
}
