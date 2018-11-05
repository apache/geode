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
package batterytest.greplogs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogConsumer {
  private final List expectedExceptions = new ArrayList();
  private boolean skipLogMsgs = false;
  private boolean infoMsgFlag = false;
  private int eatLines = 0;
  private boolean tmpErrFlag = false;
  private int tmpErrLines = 0;
  private boolean saveFlag = false;
  private int savelinenum = 0;
  private final List testExpectStrs;
  StringBuilder all = null;
  private int lineNumber;
  private String fileName;
  HashMap individalErrorCount = new HashMap();
  private final int repeatLimit;

  private static final Pattern ExpectedExceptionPattern =
      Pattern.compile("<ExpectedException action=(add|remove)>(.*)</ExpectedException>");
  private static final Pattern logPattern =
      Pattern.compile("^\\[(?:fatal|error|warn|info|debug|trace|severe|warning|fine|finer|finest)");
  private static final Pattern blankPattern = Pattern.compile("^\\s*$");
  /**
   * Any messages at these levels will be skipped
   */
  private static final Pattern skipLevelPattern =
      Pattern.compile("^\\[(?:warn|warning|info|debug|trace|fine|finer|finest)");
  private static final Pattern fatalOrErrorPattern = Pattern.compile("^\\[(?:fatal|error|severe)");
  private static final Pattern causedByPattern = Pattern.compile("Caused by");
  private static final Pattern shortErrPattern =
      Pattern.compile("^\\[[^\\]]+\\](.*)$", Pattern.MULTILINE | Pattern.DOTALL);
  private static final Pattern wroteExceptionPattern =
      Pattern.compile("\\[debug.*Wrote exception:");
  private static final Pattern rmiWarnPattern = Pattern.compile(
      "^WARNING: Failed to .*java.rmi.ConnectException: Connection refused to host: .*; nested exception is:");
  private static final Pattern javaLangErrorPattern = Pattern.compile("^java\\.lang\\.\\S+Error$");
  private static final Pattern exceptionPattern = Pattern.compile("Exception:");
  private static final Pattern exceptionPattern2 =
      Pattern.compile("( [\\w\\.]+Exception: (([\\S]+ ){0,6}))");
  private static final Pattern exceptionPattern3 = Pattern.compile("( [\\w\\.]+Exception)$");
  private static final Pattern exceptionPattern4 = Pattern.compile("^([^:]+: (([\\w\"]+ ){0,6}))");
  private static final Pattern misformatedI18nMessagePattern = Pattern.compile("[^\\d]\\{\\d+\\}");
  private static final Pattern rvvBitSetMessagePattern =
      Pattern.compile("RegionVersionVector.+bsv\\d+.+bs=\\{\\d+\\}");
  /** Limit long errors to this many lines */
  private static int ERROR_BUFFER_LIMIT = 128;



  public LogConsumer(boolean skipLogMsgs, List testExpectStrs, String fileName, int repeatLimit) {
    super();
    this.skipLogMsgs = skipLogMsgs;
    this.testExpectStrs = testExpectStrs;
    this.fileName = fileName;
    this.repeatLimit = repeatLimit;
  }

  public StringBuilder consume(CharSequence line) {
    {
      lineNumber++;
      Matcher m = ExpectedExceptionPattern.matcher(line);
      if (m.find()) {
        if (m.group(1).equals("add")) {
          expectedExceptions.add(Pattern.compile(m.group(2)));
        } else {
          // assume add and remove are the only choices
          expectedExceptions.remove(Pattern.compile(m.group(2)));
        }
        return null;
      }
    }
    if (skipLogMsgs) {
      if (infoMsgFlag) {
        if (logPattern.matcher(line).find()) {
          infoMsgFlag = false;
        } else if (blankPattern.matcher(line).matches()) {
          infoMsgFlag = false;
          return null;
        } else {
          return null;
        }
      }
      if (skipLevelPattern.matcher(line).find()) {
        infoMsgFlag = true;
        return null;
      }
    }

    if (eatLines != 0) {
      eatLines--;
      return null;
    } else {
      if (saveFlag || fatalOrErrorPattern.matcher(line).find()) {
        if (!saveFlag) {
          saveFlag = true;
          tmpErrFlag = true;
          if (checkExpectedStrs(line, expectedExceptions)) {
            saveFlag = false;
            tmpErrFlag = false;
            tmpErrLines = 0;
          }
          if (tmpErrFlag) {
            tmpErrLines = 1;
            all = new StringBuilder(line);
            all.append("\n");
            savelinenum = lineNumber;
          }
        } else {
          if (causedByPattern.matcher(line).find()) {
            // This code used to stop appending if a causedBy was seen.
            // But we want the causedBy stack trace to also be included
            // in the suspect StringBuilder.
            // The main thing is we do not want to call checkExpectedStrs
            // with this "caused by" line.
          } else if (checkExpectedStrs(line, expectedExceptions)) {
            // reset the counters and throw it all away if it matches
            // one of the registered expected strings
            tmpErrFlag = false;
            tmpErrLines = 0;
            saveFlag = false;
          }

          // We save all the lines up to the next blank line so we're
          // looking for a blank line here
          if (blankPattern.matcher(line).matches()) {
            // we found a blank line so print the suspect string
            // and reset the savetag flag
            saveFlag = false;
            Matcher m = shortErrPattern.matcher(all.toString());
            if (m.matches()) {
              String shortName = m.group(1);
              Integer i = (Integer) individalErrorCount.get(shortName);
              Integer occurances = new Integer((i == null) ? 1 : i.intValue() + 1);
              individalErrorCount.put(shortName, occurances);
              return enforceErrorLimit(occurances.intValue(), all.toString(),
                  // reader.getLineNumber(),
                  savelinenum, fileName);

            } else {
              // error in determining shortName, wing it
              return enforceErrorLimit(1, all.toString(), lineNumber, fileName);
            }
          }

          // we're still saving lines to append them on to all which contains
          // all the lines we're trying to save
          if (tmpErrFlag) {
            if (tmpErrLines < ERROR_BUFFER_LIMIT) {
              tmpErrLines++;
              all.append(line).append("\n");
            }
            if (tmpErrLines == ERROR_BUFFER_LIMIT) {
              tmpErrLines++; // increment to prevent this line from repeating
              all.append("GrepLogs: ERROR_BUFFER_LIMIT limit reached,")
                  .append(" the error was too long to display completely.\n");
            }

          }
        }
        // unique condition for when cache server see log exception and
        // logging level is set to fine. Message looks like this:
        // [fine 2005/10/25 17:53:13.586 PDT gemfire2 Server connection from
        // hobbes.gemstone.com:34466-0xf4 nid=0x23e40f1] Server connection from
        // hobbes.gemstone.com:34466: Wrote exception:
        // org.apache.geode.cache.EntryNotFoundException: remote-destroy-key
        // also now handles a JMX WARNING
      } else if (wroteExceptionPattern.matcher(line).find()
          || rmiWarnPattern.matcher(line).find()) {
        // Eat only the single EntryNotFound Exception
        eatLines = 1;
        // if we are here then the line didn't have severe or error in it and
        // didn't meet any special cases that require eating lines
        // Check for other kinds of exceptions. This is by no means inclusive
        // of all types of exceptions that could occur and some ARE missed.
      } else if (exceptionPattern.matcher(line).find() || javaLangErrorPattern.matcher(line).find()
          || (misformatedI18nMessagePattern.matcher(line).find()
              && !(skipLevelPattern.matcher(line).find()
                  && rvvBitSetMessagePattern.matcher(line).find()))) {
        if (!checkExpectedStrs(line, expectedExceptions)) {
          // it's the Exception colon that we want to find
          // along with the next six words and define to shortline
          // shortline is only used for the unique sting to count the
          // number of times an exception match occurs. This is so
          // we can suppress further printing if we hit the limit
          Matcher m2 = exceptionPattern2.matcher(line);
          Matcher m3 = exceptionPattern3.matcher(line);
          Matcher m4 = exceptionPattern4.matcher(line);
          String shortName = null;

          if (m2.find()) {
            shortName = m2.group(1);
          } else if (m3.find()) {
            shortName = m3.group(1);
          } else if (m4.find()) {
            shortName = m4.group(1);
          }
          if (shortName != null) {
            Integer i = (Integer) individalErrorCount.get(shortName);
            Integer occurances = new Integer((i == null) ? 1 : i.intValue() + 1);
            individalErrorCount.put(shortName, occurances);
            return enforceErrorLimit(occurances.intValue(), line + "\n", lineNumber, fileName);
          } else {
            return enforceErrorLimit(1, line + "\n", lineNumber, fileName);
          }
        }
      }
    }

    return null;
  }

  public StringBuilder close() {
    if (saveFlag) {
      // Bug fix for severe that occurs at the end of a log file. Since we
      // collect lines up to a blank line that never happens this prints the
      // collection of in process suspect strings if we close the file and
      // we're still trying to save lines

      saveFlag = false;
      return enforceErrorLimit(1, all.toString(), savelinenum, fileName);
    }
    return null;
  }

  private boolean checkExpectedStrs(CharSequence line, List expectedExceptions) {
    for (int i = 0; i < expectedExceptions.size(); i++) {
      Pattern p = (Pattern) expectedExceptions.get(i);
      if (p.matcher(line).find())
        return true;
    }
    for (int i = 0; i < testExpectStrs.size(); i++) {
      Pattern p = (Pattern) testExpectStrs.get(i);
      if (p.matcher(line).find())
        return true;
    }
    return false;
  }

  private StringBuilder enforceErrorLimit(int hits, String line, int linenum, String filename) {
    if (hits <= repeatLimit) {
      StringBuilder buffer = new StringBuilder();
      buffer.append("-----------------------------------------------------------------------\n")
          .append("Found suspect string in ").append(filename).append(" at line ").append(linenum)
          .append("\n\n").append(line).append("\n");
      return buffer;
    }
    if (hits == repeatLimit) {
      StringBuilder buffer = new StringBuilder();
      buffer.append("\n\nHit occurrence limit of ").append(hits).append(" for this string.\n")
          .append("Further reporting of this type of error will be suppressed.\n");
      return buffer;
    }
    return null;
  }

}
