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

import static java.lang.System.lineSeparator;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.ExitCode;

/**
 * Parses a log file written by a {@link LogWriter} into {@link LogFileParser.LogEntry}s. It
 * behaves sort of like an {@link StringTokenizer}.
 *
 * @since GemFire 3.0
 */
public class LogFileParser {

  private static final boolean TRIM_TIMESTAMPS = Boolean.getBoolean("mergelogs.TRIM_TIMESTAMPS");

  private static final boolean NEWLINE_AFTER_HEADER =
      Boolean.getBoolean("mergelogs.NEWLINE_AFTER_HEADER");

  private static final boolean TRIM_NAMES = Boolean.getBoolean("mergelogs.TRIM_NAMES");

  /** Text that signifies the start of a JRockit-style thread dump */
  private static final String FULL_THREAD_DUMP = "===== FULL THREAD DUMP ===============";

  /** The name of the log file being parsed */
  private final String logFileName;

  /** The name of the log file plus a colon and space */
  private final String extLogFileName;

  /** The buffer to read the log file from */
  private final BufferedReader br;

  /** Are there more entries to parser? */
  private boolean hasMoreEntries;

  /** The timestamp of the entry being parsed */
  private String timestamp;

  /** StringBuffer containing the text of the entry we're parsing */
  private StringBuffer sb;

  /** whether we're still reading the first line of the first entry */
  private boolean firstEntry = true;

  /**
   * StringBuffer containing white space that is the same length as logFileName plus ": ", in a
   * monospace font when tabs are 8 chars long
   */
  private final StringBuffer whiteFileName;

  /** whether to suppress blank lines in output */
  private final boolean suppressBlanks;

  /**
   * Creates a new {@code LogFileParser} that reads a log from a given
   * {@code BufferedReader}. Blanks are not suppressed, and non-timestamped lines are emitted
   * as-is.
   *
   * @param logFileName The name of the log file being parsed. This is appended to the entry. If
   *        {@code logFileName} is {@code null} nothing will be appended.
   * @param br Where to read the log from
   */
  public LogFileParser(final String logFileName, final BufferedReader br) {
    this(logFileName, br, false, false);
  }

  /**
   * Creates a new {@code LogFileParser} that reads a log from a given
   * {@code BufferedReader}.
   *
   * @param logFileName The name of the log file being parsed. This is appended to the entry. If
   *        {@code logFileName} is {@code null} nothing will be appended.
   * @param br Where to read the log from
   * @param tabOut Whether to add white-space to non-timestamped lines to align them with lines
   *        containing file names.
   * @param suppressBlanks whether to suppress blank lines
   */
  public LogFileParser(final String logFileName, final BufferedReader br, final boolean tabOut,
      final boolean suppressBlanks) {
    this.logFileName = logFileName;
    this.br = br;
    hasMoreEntries = true;
    timestamp = null;
    sb = new StringBuffer();
    this.suppressBlanks = suppressBlanks;
    whiteFileName = new StringBuffer();
    if (tabOut) {
      int numTabs = (logFileName.length() + 2) / 8;
      for (int i = 0; i < numTabs; i++) {
        whiteFileName.append('\t');
      }
      for (int i = (logFileName.length() + 2) % 8; i > 0; i--) {
        whiteFileName.append(' ');
      }
    }
    if (this.logFileName != null) {
      extLogFileName = this.logFileName + ": ";
    } else {
      extLogFileName = null;
    }
  }

  /**
   * Returns whether or not there are any more entries in the file to be parser.
   */
  public boolean hasMoreEntries() {
    return hasMoreEntries;
  }

  /**
   * copy the timestamp out of a log entry, if there is one, and return it. if there isn't a
   * timestamp, return null
   */
  private String getTimestamp(final String line) {
    int llen = line.length();
    String result = null;
    if (llen > 10) {
      // first see if the start of the line is a timestamp, as in a thread-dump's stamp
      if (line.charAt(0) == '2' && line.charAt(1) == '0' && line.charAt(4) == '-'
          && line.charAt(7) == '-') {
        return line.substring(0, 19).replace('-', '/');
      }
      // now look for gemfire's log format
      if (line.charAt(0) == '[') {
        if (line.charAt(1) == 'i' && line.charAt(2) == 'n'
            && line.charAt(3) == 'f' ||

            line.charAt(1) == 'f' && line.charAt(2) == 'i'
                && line.charAt(3) == 'n'
            ||

            line.charAt(1) == 'w' && line.charAt(2) == 'a'
                && line.charAt(3) == 'r'
            ||

            line.charAt(1) == 'd' && line.charAt(2) == 'e'
                && line.charAt(3) == 'b'
            ||

            line.charAt(1) == 't' && line.charAt(2) == 'r'
                && line.charAt(3) == 'a'
            ||

            line.charAt(1) == 's' && line.charAt(2) == 'e'
                && line.charAt(3) == 'v'
            ||

            line.charAt(1) == 'c' && line.charAt(2) == 'o'
                && line.charAt(3) == 'n'
            ||

            line.charAt(1) == 'e' && line.charAt(2) == 'r'
                && line.charAt(3) == 'r'
            ||

            line.charAt(1) == 's' && line.charAt(2) == 'e' && line.charAt(3) == 'c'
                && line.charAt(4) == 'u' && line.charAt(5) == 'r') {
          int sidx = 4;
          while (sidx < llen && line.charAt(sidx) != ' ') {
            sidx++;
          }
          int endIdx = sidx + 24;
          if (endIdx < llen) {
            result = line.substring(sidx + 1, endIdx + 1);
          }
        }
      }
    }
    return result;
  }

  /**
   * Returns the next entry in the log file. The last entry will be an instance of
   * {@link LogFileParser.LastLogEntry}.
   */
  public LogEntry getNextEntry() throws IOException {
    LogEntry entry = null;

    while (br.ready()) {
      String lineStr = br.readLine();
      if (lineStr == null) {
        break;
      }
      int llen = lineStr.length();
      int lend = llen;
      if (suppressBlanks || firstEntry) {
        // trim the end of the line
        while (lend > 1 && Character.isWhitespace(lineStr.charAt(lend - 1))) {
          lend--;
        }
        if (lend == 0) {
          continue;
        }
      }

      StringBuilder line = new StringBuilder(lineStr);
      if (lend != llen) {
        line.setLength(lend);
        llen = lend;
      }

      // Matcher matcher = pattern.matcher(line);
      String nextTimestamp = getTimestamp(lineStr);

      // See if we've found the beginning of a new log entry. If so, bundle
      // up the current string buffer and return it in a LogEntry representing
      // the currently parsed text
      if (nextTimestamp != null) {

        if (timestamp != null && TRIM_TIMESTAMPS) {
          int tsl = timestamp.length();
          if (tsl > 0) {
            // find where the year/mo/dy starts and delete it and the time zone
            int start = 5;
            if (line.charAt(start) != ' ') // info & fine
            {
              if (line.charAt(++start) != ' ') // finer & error
              {
                if (line.charAt(++start) != ' ') // finest, severe, config
                {
                  if (line.charAt(++start) != ' ') // warning
                  {
                    start = 0;
                  }
                }
              }
            }
            if (start > 0) {
              line.delete(start + 25, start + 29); // time zone
              line.delete(start, start + 11); // date
              if (TRIM_NAMES) {
                int idx2 = line.indexOf("<", +12);
                if (idx2 > start + 13) {
                  line.delete(start + 13, idx2 - 1);
                }
              }
            }
          }
          if (NEWLINE_AFTER_HEADER) {
            int idx = line.indexOf("tid=");
            if (idx > 0) {
              idx = line.indexOf("]", idx + 4);
              if (idx + 1 < line.length()) {
                line.insert(idx + 1, lineSeparator() + " ");
              }
            }
          }
        }

        if (timestamp != null) {
          entry = new LogEntry(timestamp, sb.toString(), suppressBlanks);
        }

        timestamp = nextTimestamp;

        if (!firstEntry) {
          sb = new StringBuffer(500);
        } else {
          firstEntry = false;
        }
        if (extLogFileName != null) {
          sb.append(extLogFileName);
        }

      } else if (line.indexOf(FULL_THREAD_DUMP) != -1) {
        // JRockit-style thread dumps have time stamps!
        String dump = lineStr;
        lineStr = br.readLine();
        if (lineStr == null) {
          break;
        }
        DateFormat df = DateFormatter.createDateFormat("E MMM d HH:mm:ss yyyy");
        df.setLenient(true);
        try {
          Date date = df.parse(lineStr);

          if (timestamp != null) {
            // We've found the end of a log entry
            entry = new LogEntry(timestamp, sb.toString());
          }

          df = DateFormatter.createDateFormat();
          timestamp = df.format(date);
          lineStr = dump;

          sb = new StringBuffer();
          if (extLogFileName != null) {
            sb.append(extLogFileName);
          }
          sb.append("[dump ");
          sb.append(timestamp);
          sb.append("]").append(lineSeparator()).append(lineSeparator());

        } catch (ParseException ex) {
          // Oh well...
          sb.append(dump);
        }
      } else {
        sb.append(whiteFileName);
      }

      sb.append(line);
      sb.append(lineSeparator());

      if (entry != null) {
        return entry;
      }
    }

    if (timestamp == null) {
      // The file didn't contain any log entries. Just use the
      // current time
      DateFormat df = DateFormatter.createDateFormat();
      // Date now = new Date();
      timestamp = df.format(new Date());

      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);

      LocalLogWriter tempLogger = new LocalLogWriter(ALL.intLevel(), pw);
      tempLogger.info("MISSING TIME STAMP");
      pw.flush();
      sb.insert(0, lineSeparator() + lineSeparator());
      sb.insert(0, sw.toString().trim());
      sb.insert(0, extLogFileName);
    }

    // Place the final log entry
    entry = new LastLogEntry(timestamp, sb.toString());
    sb = null;
    hasMoreEntries = false;
    return entry;
  }

  /**
   * Main program that simply parses a log file and prints out the entries. It is used for testing
   * purposes.
   */
  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("** Missing log file name");
      ExitCode.FATAL.doSystemExit();
    }

    String logFileName = args[0];
    BufferedReader br = new BufferedReader(new FileReader(logFileName));
    LogFileParser parser = new LogFileParser(logFileName, br, false, false);
    PrintWriter pw = new PrintWriter(System.out);
    while (parser.hasMoreEntries()) {
      LogEntry entry = parser.getNextEntry();
      entry.writeTo(pw);
    }
  }

  /**
   * A parsed entry in a log file. Note that we maintain the entry's timestamp as a
   * {@code String}. {@link DateFormat#parse(String) Parsing} it was too
   * expensive.
   */
  static class LogEntry {

    /** Timestamp of the log entry */
    private final String timestamp;

    /** The contents of the log entry */
    private final String contents;

    /** whether extraneous blank lines are being suppressed */
    private boolean suppressBlanks;

    /**
     * Creates a new log entry with the given timestamp and contents
     */
    public LogEntry(final String timestamp, final String contents) {
      this.timestamp = timestamp;
      this.contents = contents;
    }

    /**
     * Creates a new log entry with the given timestamp and contents
     */
    public LogEntry(final String timestamp, final String contents, final boolean suppressBlanks) {
      this.timestamp = timestamp;
      this.contents = contents.trim();
      this.suppressBlanks = suppressBlanks;
    }

    /**
     * Returns the timestamp of this log entry
     */
    public String getTimestamp() {
      return timestamp;
    }

    /**
     * Returns the contents of this log entry
     *
     * @see #writeTo
     */
    String getContents() {
      return contents;
    }

    /**
     * Writes the contents of this log entry to a {@code PrintWriter}.
     */
    public void writeTo(final PrintWriter pw) {
      pw.println(contents);
      if (!suppressBlanks) {
        pw.println("");
      }
      pw.flush();
    }

    /**
     * Is this entry the last log entry?
     */
    public boolean isLast() {
      return false;
    }
  }

  /**
   * The last log entry read from a log file. We use a separate class to avoid the overhead of an
   * extra {@code boolean} field in each {@link LogFileParser.LogEntry}.
   */
  static class LastLogEntry extends LogEntry {

    public LastLogEntry(final String timestamp, final String contents) {
      super(timestamp, contents);
    }

    @Override
    public boolean isLast() {
      return true;
    }
  }
}
