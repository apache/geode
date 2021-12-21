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
package org.apache.persistence.admin;

import java.io.PrintWriter;
import java.text.BreakIterator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Provides single point for all log messages to written to. Currently this class only supports
 * static methods and always writes to stdout.
 *
 */
public class Logger {
  private static final PrintWriter logWriter = new PrintWriter(System.out, true);

  // Set LOGWIDTH to maxint as a cheap way of turning off formatting
  private static final int LOGWIDTH = Integer.MAX_VALUE;
  private static final SimpleDateFormat timeFormatter;
  static {
    final String defaultFormatPattern = "MM/dd/yy HH:mm:ss.SSS z";
    final String resourceName = "org.apache.persistence.admin.LoggerResources";
    final String keyName = "logger.timeStampFormat";
    String formatPattern = defaultFormatPattern;
    SimpleDateFormat sdf;
    try {
      ResourceBundle messageRB = ResourceBundle.getBundle(resourceName);
      try {
        formatPattern = messageRB.getString(keyName);
      } catch (MissingResourceException e) {
        System.out.println("NOTICE: Logger using default timestamp format."
            + " Could not get resource key \"" + keyName + "\" because: " + e);
      }
    } catch (MissingResourceException e) {
      System.out.println("NOTICE: Logger using default timestamp format."
          + " Could not load resource bundle \"" + resourceName + "\" because: " + e);
    }
    if (formatPattern.length() == 0) {
      sdf = null;
    } else {
      try {
        sdf = new SimpleDateFormat(formatPattern);
      } catch (RuntimeException e) {
        System.out.println("NOTICE: ignoring timestamp pattern \"" + formatPattern + "\" because: "
            + e);
        System.out.println("  Using default pattern: \"" + defaultFormatPattern + "\".");
        formatPattern = defaultFormatPattern;
        sdf = new SimpleDateFormat(formatPattern);
      }
    }
    timeFormatter = sdf;
  }

  private static void formatText(PrintWriter writer, String target, int maxLength,
      int initialLength) {
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
      } else if (endChar == '\t') {
        // figure tabs use 8 characters
        lineLength += 7;
      }
      String word = target.substring(start, wordEnd);
      if ((lineLength + word.length()) >= maxLength) {
        if (lineLength != 0) {
          writer.println();
          writer.print("  ");
          lineLength = 2;
        }
      }
      lineLength += word.length();
      writer.print(word);
      if (endChar == '\n') {
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

  /**
   * Gets a String representation of the current time.
   *
   * @return a String representation of the current time.
   */
  public static String getTimeStamp() {
    return formatDate(new Date());
  }

  /**
   * Convert a Date to a timestamp String.
   *
   * @param d a Date to format as a timestamp String.
   * @return a String representation of the current time.
   */
  public static String formatDate(Date d) {
    if (timeFormatter == null) {
      try {
        // very simple format that shows millisecond resolution
        return Long.toString(d.getTime());
      } catch (Exception ignore) {
        return "timestampFormatFailed";
      }
    }
    try {
      synchronized (timeFormatter) {
        // Need sync: see bug 21858
        return timeFormatter.format(d);
      }
    } catch (Exception e1) {
      // Fix bug 21857
      try {
        return d.toString();
      } catch (Exception e2) {
        try {
          return Long.toString(d.getTime());
        } catch (Exception e3) {
          return "timestampFormatFailed";
        }
      }
    }
  }

  /**
   * Logs a message to the static log destination.
   *
   * @param msg the actual message to log
   */
  public static void put(String msg) {
    put(msg, null);
  }

  /**
   * Logs a message to the specified log destination.
   *
   * @param log the <code>PrintWriter</code> that the message will be written to.
   * @param msg the actual message to log
   */
  public static void put(PrintWriter log, String msg) {
    put(log, msg, null);
  }

  /**
   * Logs an exception to the static log destination.
   *
   * @param exception the actual Exception to log
   */
  public static void put(Throwable exception) {
    put((String) null, exception);
  }

  /**
   * Logs an exception to the specified log destination.
   *
   * @param log the <code>PrintWriter</code> that the message will be written to.
   * @param exception the actual Exception to log
   */
  public static void put(PrintWriter log, Throwable exception) {
    put(log, null, exception);
  }

  /**
   * Logs a message and an exception to the static log destination.
   *
   * @param msg the actual message to log
   * @param exception the actual Exception to log
   */
  public static void put(String msg, Throwable exception) {
    put(logWriter, msg, exception);
  }

  /**
   * Logs a message and an exception to the specified log destination.
   *
   * @param log the <code>PrintWriter</code> that the message will be written to. If null then the
   *        default stdout writer is used.
   * @param msg the actual message to log
   * @param exception the actual Exception to log
   */
  public static void put(PrintWriter log, String msg, Throwable exception) {
    java.io.StringWriter sw = new java.io.StringWriter();
    String header;
    PrintWriter pw = new PrintWriter(sw);

    pw.println();
    header = '[' + getTimeStamp() + ' ' + Thread.currentThread().getName() + "] ";
    pw.print(header);
    if (msg != null) {
      try {
        formatText(pw, msg, LOGWIDTH, header.length());
      } catch (RuntimeException e) {
        pw.println(msg);
        pw.println("Ignoring exception:");
        e.printStackTrace(pw);
      }
    } else {
      pw.println();
    }
    if (exception != null) {
      exception.printStackTrace(pw);
    }
    pw.close();
    try {
      sw.close();
    } catch (java.io.IOException ignore) {
    }

    if (log == null) {
      log = logWriter;
    }
    log.print(sw);
    log.flush();
  }

  /**
   * Formats a message. Takes special care when invoking the toString() method of objects that might
   * cause NPEs.
   */
  public static String format(String format, Object[] objs) {
    String[] strings = new String[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Object obj = objs[i];
      if (obj == null) {
        strings[i] = "null";

      } else {
        try {
          strings[i] = obj.toString();

        } catch (Exception ex) {
          strings[i] = obj.getClass().getName() + "@" + System.identityHashCode(obj);
        }
      }
    }

    return java.text.MessageFormat.format(format, (Object[]) strings);
  }

}
