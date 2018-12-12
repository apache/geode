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
package org.apache.geode.internal.admin.remote;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.log4j.LogLevel;

/**
 * Implementation of the Alert interface.
 */
public class RemoteAlert implements Alert {
  private final GemFireVM manager;
  private final String connectionName;
  private final String sourceId;
  private final int level;
  private final Date date;
  private final String message;
  private final InternalDistributedMember sender;

  public RemoteAlert(GemFireVM manager, int level, Date date, String connectionName,
      String threadName, long tid, String msg, String exceptionText,
      InternalDistributedMember sender) {
    this.manager = manager;
    this.level = level;
    this.date = date;
    this.connectionName = connectionName;
    {
      StringBuffer tmpSourceId = new StringBuffer();

      tmpSourceId.append(threadName);
      if (tmpSourceId.length() > 0) {
        tmpSourceId.append(' ');
      }
      tmpSourceId.append("tid=0x");
      tmpSourceId.append(Long.toHexString(tid));
      this.sourceId = tmpSourceId.toString();
    }
    {
      StringBuffer tmpMessage = new StringBuffer();
      tmpMessage.append(msg);
      if (tmpMessage.length() > 0) {
        tmpMessage.append('\n');
      }
      tmpMessage.append(exceptionText);
      this.message = tmpMessage.toString();
    }
    this.sender = sender;
  }

  public int getLevel() {
    return level;
  }

  public GemFireVM getGemFireVM() {
    return manager;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getMessage() {
    return message;
  }

  public Date getDate() {
    return date;
  }

  /**
   * Returns a InternalDistributedMember instance representing a member that is sending (or has
   * sent) this alert. Could be <code>null</code>.
   *
   * @return the InternalDistributedMember instance representing a member that is sending/has sent
   *         this alert
   *
   * @since GemFire 6.5
   */
  public InternalDistributedMember getSender() {
    return sender;
  }

  /**
   * Converts the String return by an invocation of {@link #toString} into an <code>Alert</code>.
   */
  public static Alert fromString(String s) {
    int firstBracket = s.indexOf('[');
    int lastBracket = s.indexOf(']');

    final String message = s.substring(lastBracket + 1).trim();

    String stamp = s.substring(firstBracket, lastBracket);
    StringTokenizer st = new StringTokenizer(stamp, "[ ");

    final int level = LogLevel.getLogWriterLevel(st.nextToken());

    StringBuffer sb = new StringBuffer();
    sb.append(st.nextToken());
    sb.append(" ");
    sb.append(st.nextToken());
    sb.append(" ");
    sb.append(st.nextToken());

    final DateFormat timeFormatter = DateFormatter.createDateFormat();
    final Date date;
    try {
      date = timeFormatter.parse(sb.toString());

    } catch (ParseException ex) {
      throw new IllegalArgumentException(
          String.format("Invalidate timestamp: %s", sb.toString()));
    }

    // Assume that the connection name is only one token...
    final String connectionName = st.nextToken();

    sb = new StringBuffer();
    while (st.hasMoreTokens()) {
      sb.append(st.nextToken());
      sb.append(" ");
    }
    final String sourceId = sb.toString().trim();

    Assert.assertTrue(!st.hasMoreTokens());

    return new Alert() {
      public int getLevel() {
        return level;
      }

      public GemFireVM getGemFireVM() {
        return null;
      }

      public String getConnectionName() {
        return connectionName;
      }

      public String getSourceId() {
        return sourceId;
      }

      public String getMessage() {
        return message;
      }

      public Date getDate() {
        return date;
      }

      public InternalDistributedMember getSender() {
        /* Not implemented, currently this is used only for testing purpose */
        return null;
      }
    };
  }

  @Override
  public String toString() {
    final DateFormat timeFormatter = DateFormatter.createDateFormat();
    java.io.StringWriter sw = new java.io.StringWriter();
    PrintWriter pw = new PrintWriter(sw);


    pw.print('[');
    pw.print(LogWriterImpl.levelToString(level));
    pw.print(' ');
    pw.print(timeFormatter.format(date));
    pw.print(' ');
    pw.print(connectionName);
    pw.print(' ');
    pw.print(sourceId);
    pw.print("] ");
    pw.print(message);

    pw.close();
    try {
      sw.close();
    } catch (java.io.IOException ignore) {
    }
    return sw.toString();
  }
}
