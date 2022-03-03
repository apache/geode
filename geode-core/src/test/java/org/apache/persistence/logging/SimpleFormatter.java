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
package org.apache.persistence.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * A Formatter that returns a textual description of a LogRecord
 */
public class SimpleFormatter extends Formatter {

  @Override
  public String format(LogRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    sb.append(org.apache.persistence.admin.Logger.formatDate(new Date(record.getMillis())));
    sb.append(' ');
    sb.append(Thread.currentThread().getName());
    sb.append("] ");
    sb.append(record.getMessage());
    sb.append('\n');

    if (record.getSourceClassName() != null) {
      sb.append(" In ");
      sb.append(record.getSourceClassName());
      if (record.getSourceMethodName() != null) {
        sb.append(".");
        sb.append(record.getSourceMethodName());
      }
      sb.append('\n');
    }

    Object[] params = record.getParameters();
    if (params != null) {
      for (final Object param : params) {
        sb.append(param);
        sb.append('\n');
      }
    }

    if (record.getThrown() != null) {
      Throwable thr = record.getThrown();
      StringWriter sw = new StringWriter();
      thr.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw);
      sb.append('\n');
    }

    if (STACK_TRACE) {
      Exception thr = new Exception("Stack Trace");
      StringWriter sw = new StringWriter();
      thr.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw);
      sb.append('\n');
    }

    sb.append('\n');

    return (sb.toString());
  }

}
