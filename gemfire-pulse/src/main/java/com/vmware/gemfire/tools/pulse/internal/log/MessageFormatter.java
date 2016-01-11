/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.log;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;

/**
 * Class MessageFormatter
 * 
 * MessageFormatter is the custom formatter class for formatting the log
 * messages.
 * 
 * @author Sachin K
 * @since version 7.0.1
 */
public class MessageFormatter extends Formatter {

  public MessageFormatter() {
    super();
  }

  @Override
  public String format(LogRecord record) {
    DateFormat df = new SimpleDateFormat(Repository.get().getPulseConfig()
        .getLogDatePattern());
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    pw.print("[");
    pw.print(record.getLevel().getName());
    pw.print(" ");
    pw.print(df.format(new Date(record.getMillis())));
    String threadName = Thread.currentThread().getName();
    if (threadName != null) {
      pw.print(" ");
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

    pw.println("[" + PulseConstants.APP_NAME + "]");

    pw.println("[" + record.getLoggerName() + "]");

    pw.println(record.getMessage());

    if (record.getThrown() != null) {
      record.getThrown().printStackTrace(pw);
    }
    pw.close();
    try {
      sw.close();
    } catch (IOException ignore) {
    }
    String result = sw.toString();
    return result;
  }

  public String getHead(Handler h) {
    return super.getHead(h);
  }

  public String getTail(Handler h) {
    return super.getTail(h);
  }
} // End of Class MessageFormatter
