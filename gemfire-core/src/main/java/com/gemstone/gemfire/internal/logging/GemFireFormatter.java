/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.logging;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Implementation of the standard JDK formatter that formats a message
 * in GemFire's log format.
 */
public class GemFireFormatter extends Formatter {

  /**
   * Use the log writer to use some of its formatting code.
   */
  private final LogWriter logWriter;
  
  private final DateFormat dateFormat = DateFormatter.createDateFormat();

  public GemFireFormatter(LogWriter logWriter) {
    this.logWriter = logWriter;
  }
    
  @Override
  public String format(LogRecord record) {
    java.io.StringWriter sw = new java.io.StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.print('[');
    pw.print(record.getLevel().getName());
    pw.print(' ');
    pw.print(dateFormat.format(new Date(record.getMillis())));
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
//     if (record.getLoggerName() != null) {
//       pw.print(' ');
//       pw.print(record.getLoggerName());
//     }
//     if (record.getSourceClassName() != null) {
//       pw.print(' ');
//       pw.print(record.getSourceClassName());
//     }
//     if (record.getSourceMethodName() != null) {
//       pw.print(' ');
//       pw.print(record.getSourceMethodName());
//     }
    pw.print(") ");

    String msg = record.getMessage();
    if (msg != null) {
      try {
        LogWriterImpl.formatText(pw, msg, 40);
      } catch (RuntimeException e) {
        pw.println(msg);
        pw.println(LocalizedStrings.GemFireFormatter_IGNORING_THE_FOLLOWING_EXCEPTION.toLocalizedString());
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
    } catch (java.io.IOException ignore) {}
    String result = sw.toString();
    return result;
  }
}
