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
package org.apache.geode.test.greplogs;

import static java.lang.System.lineSeparator;
import static java.util.regex.Pattern.compile;
import static org.apache.geode.test.greplogs.Patterns.BLANK;
import static org.apache.geode.test.greplogs.Patterns.CAUSED_BY;
import static org.apache.geode.test.greplogs.Patterns.DEBUG_WROTE_EXCEPTION;
import static org.apache.geode.test.greplogs.Patterns.ERROR_OR_MORE_LOG_LEVEL;
import static org.apache.geode.test.greplogs.Patterns.ERROR_SHORT_NAME;
import static org.apache.geode.test.greplogs.Patterns.EXCEPTION;
import static org.apache.geode.test.greplogs.Patterns.EXCEPTION_2;
import static org.apache.geode.test.greplogs.Patterns.EXCEPTION_3;
import static org.apache.geode.test.greplogs.Patterns.EXCEPTION_4;
import static org.apache.geode.test.greplogs.Patterns.HYDRA_MASTER_LOCATORS_WILDCARD;
import static org.apache.geode.test.greplogs.Patterns.IGNORED_EXCEPTION;
import static org.apache.geode.test.greplogs.Patterns.JAVA_LANG_ERROR;
import static org.apache.geode.test.greplogs.Patterns.LOG_STATEMENT;
import static org.apache.geode.test.greplogs.Patterns.MALFORMED_I18N_MESSAGE;
import static org.apache.geode.test.greplogs.Patterns.MALFORMED_LOG4J_MESSAGE;
import static org.apache.geode.test.greplogs.Patterns.RMI_WARNING;
import static org.apache.geode.test.greplogs.Patterns.RVV_BIT_SET_MESSAGE;
import static org.apache.geode.test.greplogs.Patterns.WARN_OR_LESS_LOG_LEVEL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogConsumer {

  /** Limit long errors to this many lines */
  private static final int ERROR_BUFFER_LIMIT = 128;

  private final String logName;
  private final Collection<Pattern> dynamicIgnoredPatterns = new ArrayList<>();
  private final Collection<Pattern> constantIgnoredPatterns = new ArrayList<>();
  private final boolean enableLogLevelThreshold;
  private final int skipLimit;

  private final Map<String, Integer> individualErrorCount = new HashMap<>();

  private boolean infoMsgFlag;
  private int eatLines;
  private boolean tmpErrFlag;
  private int tmpErrLines;
  private boolean saveFlag;
  private int savelinenum;
  private StringBuilder all;
  private int lineNumber;

  public LogConsumer(boolean enableLogLevelThreshold, Collection<Pattern> constantIgnoredPatterns,
      String logName,
      int repeatLimit) {
    this.enableLogLevelThreshold = enableLogLevelThreshold;
    this.constantIgnoredPatterns.addAll(constantIgnoredPatterns);
    this.logName = logName;
    this.skipLimit = repeatLimit;
  }

  public String consume(CharSequence line) {
    lineNumber++;

    // IgnoredException injects lines into the log to start or end ignore periods.
    // Process those lines, then exit.
    Matcher expectedExceptionMatcher = IGNORED_EXCEPTION.matcher(line);
    if (expectedExceptionMatcher.find()) {
      expectedExceptionMatcherHandler(expectedExceptionMatcher);
      return null;
    }

    // We may optionally skip info-level logs
    if (enableLogLevelThreshold && skipLine(line)) {
      return null;
    }

    // In some case, we want to skip an extra line.
    if (eatLines != 0) {
      eatLines--;
      return null;
    }

    if (saveFlag ||
        ERROR_OR_MORE_LOG_LEVEL.matcher(line).find() ||
        isExceptionErrorOrSomeSpecialCase(line)) {
      if (!saveFlag) {
        setInstanceVariablesForSomeReason(line);
      } else {
        if (!CAUSED_BY.matcher(line).find() && matchesIgnoredPatterns(line)) {
          // reset the counters and throw it all away if it matches
          // one of the registered expected strings
          tmpErrFlag = false;
          tmpErrLines = 0;
          saveFlag = false;
        }

        // We save all the lines up to the next blank line so we're
        // looking for a blank line here
        if (BLANK.matcher(line).matches()) {
          return enforceErrorLimitsAtShortErrMatcher();
        }

        // we're still saving lines to append them on to all which contains
        // all the lines we're trying to save
        if (tmpErrFlag) {
          addErrLinesToAll(line);
        }
      }

    } else if (isWroteOrRMIWarn(line)) {
      handleWroteOrRMIWarn();
      return null;
    }

    return null;
  }

  public String close() {
    if (saveFlag) {
      saveFlag = false;
      return enforceErrorLimit(1, all.toString(), savelinenum, logName);
    }
    return null;
  }

  private void handleWroteOrRMIWarn() {
    // unique condition for when cache server see log exception and
    // logging level is set to fine. Message looks like this:
    // [fine 2005/10/25 17:53:13.586 PDT gemfire2 Server connection from
    // hobbes.gemstone.com:34466-0xf4 nid=0x23e40f1] Server connection from
    // hobbes.gemstone.com:34466: Wrote exception:
    // org.apache.geode.cache.EntryNotFoundException: remote-destroy-key
    // also now handles a JMX WARNING

    // if we are here then the line didn't have severe or error in it and
    // didn't meet any special cases that require eating lines
    // Check for other kinds of exceptions. This is by no means inclusive
    // of all types of exceptions that could occur and some ARE missed.

    // Eat only the single EntryNotFound Exception
    eatLines = 1;
  }

  private boolean isWroteOrRMIWarn(CharSequence line) {
    return DEBUG_WROTE_EXCEPTION.matcher(line).find() || RMI_WARNING.matcher(line).find();
  }

  private boolean isExceptionErrorOrSomeSpecialCase(CharSequence line) {
    return (EXCEPTION.matcher(line).find() ||
        JAVA_LANG_ERROR.matcher(line).find() ||
        MALFORMED_I18N_MESSAGE.matcher(line).find() ||
        MALFORMED_LOG4J_MESSAGE.matcher(line).find()) &&
        !(HYDRA_MASTER_LOCATORS_WILDCARD.matcher(line).find()) &&
        !(WARN_OR_LESS_LOG_LEVEL.matcher(line).find() &&
            RVV_BIT_SET_MESSAGE.matcher(line).find());
  }

  private void addErrLinesToAll(CharSequence line) {
    if (tmpErrLines < ERROR_BUFFER_LIMIT) {
      tmpErrLines++;
      all.append(line).append(lineSeparator());
    }
    if (tmpErrLines == ERROR_BUFFER_LIMIT) {
      tmpErrLines++; // increment to prevent this line from repeating
      all.append("GrepLogs: ERROR_BUFFER_LIMIT limit reached,")
          .append(" the error was too long to display completely.").append(lineSeparator());
    }
  }

  private String enforceErrorLimitsAtShortErrMatcher() {
    // we found a blank line so print the suspect string and reset the savetag flag
    saveFlag = false;

    Matcher shortErrMatcher = ERROR_SHORT_NAME.matcher(all.toString());
    if (shortErrMatcher.matches()) {
      String shortName = shortErrMatcher.group(1);
      Integer i = individualErrorCount.get(shortName);
      int occurrences = i == null ? 1 : i + 1;
      individualErrorCount.put(shortName, occurrences);
      return enforceErrorLimit(occurrences, all.toString(), savelinenum, logName);
    }

    // error in determining shortName, wing it
    return enforceErrorLimit(1, all.toString(), lineNumber, logName);
  }

  private void setInstanceVariablesForSomeReason(CharSequence line) {
    saveFlag = true;
    tmpErrFlag = true;
    if (matchesIgnoredPatterns(line)) {
      saveFlag = false;
      tmpErrFlag = false;
      tmpErrLines = 0;
    }
    if (tmpErrFlag) {
      tmpErrLines = 1;
      all = new StringBuilder(line);
      all.append(lineSeparator());
      savelinenum = lineNumber;
    }
  }

  private String getShortName(CharSequence line) {
    Matcher exception2Matcher = EXCEPTION_2.matcher(line);
    if (exception2Matcher.find()) {
      return exception2Matcher.group(1);
    }

    Matcher exception3Matcher = EXCEPTION_3.matcher(line);
    if (exception3Matcher.find()) {
      return exception3Matcher.group(1);
    }

    Matcher exception4Matcher = EXCEPTION_4.matcher(line);
    if (exception4Matcher.find()) {
      return exception4Matcher.group(1);
    }

    return null;
  }

  /** This method returns true if this line should be skipped. */
  private boolean skipLine(CharSequence line) {
    if (infoMsgFlag) {
      if (LOG_STATEMENT.matcher(line).find()) {
        infoMsgFlag = false;
      } else {
        if (BLANK.matcher(line).matches()) {
          infoMsgFlag = false;
        }
        return true;
      }
    }

    if (WARN_OR_LESS_LOG_LEVEL.matcher(line).find()) {
      infoMsgFlag = true;
      return true;
    }

    return false;
  }

  private void expectedExceptionMatcherHandler(MatchResult expectedExceptionMatcher) {
    if (expectedExceptionMatcher.group(1).equals("add")) {
      dynamicIgnoredPatterns.add(compile(expectedExceptionMatcher.group(2)));
    } else {
      // assume add and remove are the only choices
      dynamicIgnoredPatterns.remove(compile(expectedExceptionMatcher.group(2)));
    }
  }

  private boolean matchesIgnoredPatterns(CharSequence line) {
    return dynamicIgnoredPatterns.stream().anyMatch(p -> p.matcher(line).find()) ||
        constantIgnoredPatterns.stream().anyMatch(p -> p.matcher(line).find());
  }

  private String enforceErrorLimit(int hits, String line, int linenum, String filename) {
    if (hits < skipLimit) {
      String string = "-----------------------------------------------------------------------"
          + lineSeparator()
          + "Found suspect string in '" + filename + "' at line " + linenum
          + lineSeparator() + lineSeparator()
          + line + lineSeparator();
      return string;
    }

    if (hits == skipLimit) {
      String string = lineSeparator() + lineSeparator()
          + "Hit occurrence limit of " + hits + " for this string."
          + lineSeparator()
          + "Further reporting of this type of error will be suppressed."
          + lineSeparator();
      return string;
    }
    return null;
  }
}
