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
 *
 */

package org.apache.geode.management.internal.cli.util;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class LogFilter {
  public enum LineFilterResult {
    LINE_ACCEPTED, LINE_REJECTED, REMAINDER_OF_FILE_REJECTED
  }

  private static final Logger LOGGER = LogService.getLogger();

  private final Level thisLogLevel;
  private final boolean thisLevelOnly;
  private final LocalDateTime startDate;
  private final LocalDateTime endDate;

  private LineFilterResult resultOfPreviousLine = LineFilterResult.LINE_ACCEPTED;

  public LogFilter(Level logLevel, LocalDateTime startDate, LocalDateTime endDate) {
    this(logLevel, false, startDate, endDate);
  }

  public LogFilter(Level logLevel, boolean thisLevelOnly, LocalDateTime startDate,
      LocalDateTime endDate) {
    assert logLevel != null;
    thisLogLevel = logLevel;
    this.thisLevelOnly = thisLevelOnly;
    this.startDate = startDate;
    this.endDate = endDate;
  }

  public void startNewFile() {
    resultOfPreviousLine = LineFilterResult.LINE_ACCEPTED;
  }

  public LineFilterResult acceptsLine(String logLine) {
    LogLevelExtractor.Result result = LogLevelExtractor.extract(logLine);

    return acceptsLogEntry(result);
  }

  protected LineFilterResult acceptsLogEntry(LogLevelExtractor.Result result) {
    if (result == null) {
      return resultOfPreviousLine;
    }

    return acceptsLogEntry(result.getLogLevel(), result.getLogTimestamp());
  }

  protected LineFilterResult acceptsLogEntry(Level logLevel, LocalDateTime logTimestamp) {
    if (logTimestamp == null || logLevel == null) {
      throw new IllegalArgumentException();
    }

    LineFilterResult result;

    if (endDate != null && logTimestamp.isAfter(endDate)) {
      result = LineFilterResult.REMAINDER_OF_FILE_REJECTED;
    } else if (startDate != null && logTimestamp.isBefore(startDate)) {
      result = LineFilterResult.LINE_REJECTED;
    } else {
      if (thisLevelOnly) {
        result = logLevel.intLevel() == thisLogLevel.intLevel() ? LineFilterResult.LINE_ACCEPTED
            : LineFilterResult.LINE_REJECTED;
      } else {
        result = logLevel.isMoreSpecificThan(thisLogLevel) ? LineFilterResult.LINE_ACCEPTED
            : LineFilterResult.LINE_REJECTED;
      }
    }

    resultOfPreviousLine = result;

    return result;
  }

  public boolean acceptsFile(Path file) {
    if (startDate == null) {
      return true;
    }

    return getEndTimeOf(file).isAfter(startDate);

  }

  private static LocalDateTime getEndTimeOf(Path file) {
    try {
      long lastModifiedMillis = file.toFile().lastModified();
      return Instant.ofEpochMilli(lastModifiedMillis).atZone(ZoneId.systemDefault())
          .toLocalDateTime();
    } catch (Exception e) {
      LOGGER.error("Unable to determine lastModified time", e);
      return LocalDateTime.MAX;
    }
  }

}
