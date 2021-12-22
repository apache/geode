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
package org.apache.geode.management.internal;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Date;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.internal.logging.LogWriterImpl;

public class AlertDetails {

  private final int alertLevel;

  private final String connectionName;
  private final String threadName;
  private final long tid;
  private final String msg;
  private final String exceptionText;
  private final Date msgDate;
  private final String sourceId;
  private final String message;

  private final InternalDistributedMember sender;

  public AlertDetails(int alertLevel, Date msgDate, String connectionName, String threadName,
      long tid, String msg, String exceptionText, InternalDistributedMember sender) {

    this.alertLevel = alertLevel;
    this.connectionName = connectionName;
    this.threadName = threadName;
    this.tid = tid;
    this.msg = msg;
    this.exceptionText = exceptionText;
    this.msgDate = msgDate;
    this.sender = sender;

    {
      StringBuilder tmpSourceId = new StringBuilder();

      tmpSourceId.append(threadName);
      if (tmpSourceId.length() > 0) {
        tmpSourceId.append(' ');
      }
      tmpSourceId.append("tid=0x");
      tmpSourceId.append(Long.toHexString(tid));
      sourceId = tmpSourceId.toString();
    }
    {
      StringBuilder tmpMessage = new StringBuilder();
      tmpMessage.append(msg);
      if (tmpMessage.length() > 0) {
        tmpMessage.append('\n');
      }
      tmpMessage.append(exceptionText);
      message = tmpMessage.toString();
    }
  }

  public int getAlertLevel() {
    return alertLevel;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public String getThreadName() {
    return threadName;
  }

  public long getTid() {
    return tid;
  }

  public String getMsg() {
    return msg;
  }

  public String getExceptionText() {
    return exceptionText;
  }

  public Date getMsgTime() {
    return msgDate;
  }

  public String getSource() {
    return sourceId;
  }

  /**
   * Returns the sender of this message. Note that this value is not set until this message is
   * received by a distribution manager.
   */
  public InternalDistributedMember getSender() {
    return sender;
  }

  public String toString() {
    final DateFormat timeFormatter = DateFormatter.createDateFormat();
    java.io.StringWriter sw = new java.io.StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.print('[');
    pw.print(LogWriterImpl.levelToString(alertLevel));
    pw.print(' ');
    pw.print(timeFormatter.format(msgDate));
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

  /**
   * Converts the int alert level to a string representation.
   *
   * @param intLevel int alert level to convert
   * @return A string representation of the alert level
   */
  public static String getAlertLevelAsString(final int intLevel) {
    if (intLevel == Alert.SEVERE) {
      return "severe";
    } else if (intLevel == Alert.ERROR) {
      return "error";
    } else if (intLevel == Alert.WARNING) {
      return "warning";
    } else if (intLevel == Alert.OFF) {
      return "none";
    }

    throw new IllegalArgumentException("Unable to find an alert level with int value: " + intLevel);
  }
}
