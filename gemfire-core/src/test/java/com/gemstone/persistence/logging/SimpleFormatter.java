/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.persistence.logging;

import java.io.*;
//import java.text.*;
import java.util.*;

/**
 * A Formatter that returns a textual description of a LogRecord
 */
public class SimpleFormatter extends Formatter {

  public String format(LogRecord record) {
    StringBuffer sb = new StringBuffer();
    sb.append('[');
    sb.append(com.gemstone.persistence.admin.Logger.formatDate(new Date(record.getMillis())));
    sb.append(' ');
    sb.append(Thread.currentThread().getName());
    sb.append("] ");
    sb.append(record.getMessage());
    sb.append('\n');

    if(record.getSourceClassName() != null) {
      sb.append(" In ");
      sb.append(record.getSourceClassName());
      if(record.getSourceMethodName() != null) {
        sb.append(".");
        sb.append(record.getSourceMethodName());
      }
      sb.append('\n');
    }

    Object[] params = record.getParameters();
    if(params != null) {
      for(int i = 0; i < params.length; i++) {
        sb.append(params[i]);
        sb.append('\n');
      }
    }

    if(record.getThrown() != null) {
      Throwable thr = record.getThrown();
      StringWriter sw = new StringWriter();
      thr.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw.toString());
      sb.append('\n');
    }

    if (STACK_TRACE) {
      Exception thr = new Exception("Stack Trace");
      StringWriter sw = new StringWriter();
      thr.printStackTrace(new PrintWriter(sw, true));
      sb.append(sw.toString());
      sb.append('\n');
    }

    sb.append('\n');

    return(sb.toString());
  }

}
