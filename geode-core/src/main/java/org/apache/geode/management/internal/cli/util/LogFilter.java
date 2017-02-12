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

import static java.util.stream.Collectors.toSet;

import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Set;

public class LogFilter {
  public enum LineFilterResult {
    LINE_ACCEPTED, LINE_REJECTED, REMAINDER_OF_FILE_REJECTED
  }

  private static final Logger LOGGER = LogService.getLogger();

  private final Set<String> permittedLogLevels;
  private final LocalDateTime startDate;
  private final LocalDateTime endDate;

  private LineFilterResult resultOfPreviousLine = LineFilterResult.LINE_ACCEPTED;

  public LogFilter(Set<String> permittedLogLevels, LocalDateTime startDate, LocalDateTime endDate) {
    this.permittedLogLevels = (permittedLogLevels == null || permittedLogLevels.isEmpty())
        ? allLogLevels() : permittedLogLevels;
    this.startDate = startDate;
    this.endDate = endDate;
  }

  public void startNewFile() {
    this.resultOfPreviousLine = LineFilterResult.LINE_ACCEPTED;
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

  protected LineFilterResult acceptsLogEntry(String logLevel, LocalDateTime logTimestamp) {
    if (logTimestamp == null || logLevel == null) {
      throw new IllegalArgumentException();
    }

    LineFilterResult result;

    if (endDate != null && logTimestamp.isAfter(endDate)) {
      result = LineFilterResult.REMAINDER_OF_FILE_REJECTED;
    } else if (startDate != null && logTimestamp.isBefore(startDate)) {
      result = LineFilterResult.LINE_REJECTED;
    } else {
      result = permittedLogLevels.contains(logLevel) ? LineFilterResult.LINE_ACCEPTED
          : LineFilterResult.LINE_REJECTED;
    }

    resultOfPreviousLine = result;

    return result;
  }

  public boolean acceptsFile(Path file) {
    if (startDate == null) {
      return true;
    }
    try {
      return (getEndTimeOf(file).isAfter(startDate));
    } catch (IOException e) {
      LOGGER.error("Unable to determine lastModified time", e);
      return true;
    }
  }

  private static LocalDateTime getEndTimeOf(Path file) throws IOException {
    long lastModifiedMillis = file.toFile().lastModified();
    return Instant.ofEpochMilli(lastModifiedMillis).atZone(ZoneId.systemDefault())
        .toLocalDateTime();
  }

  public static Set<String> allLogLevels() {
    return Arrays.stream(InternalLogWriter.levelNames).collect(toSet());
  }
}
