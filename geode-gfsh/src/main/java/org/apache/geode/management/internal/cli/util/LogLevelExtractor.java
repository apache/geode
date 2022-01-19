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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.logging.internal.log4j.LogLevel;

/**
 * this will extract convert the deprecated InternalLogWriter's level into log4j level as well.
 */
public class LogLevelExtractor {
  private static final Pattern LOG_PATTERN =
      Pattern.compile("^\\[(\\S*)\\s+([\\d\\/]+)\\s+([\\d:\\.]+)\\s+(\\S+)");

  @Immutable
  private static final DateTimeFormatter LOG_TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS zzz");
  private static final String SPACE = " ";

  public static Result extract(String logLine) {
    Matcher m = LOG_PATTERN.matcher(logLine);
    if (!m.find()) {
      return null;
    }

    String logLevel = m.group(1);
    String logTimestamp = m.group(2) + SPACE + m.group(3) + SPACE + m.group(4);

    LocalDateTime timestamp = LocalDateTime.parse(logTimestamp, LOG_TIMESTAMP_FORMATTER);

    return new Result(logLevel, timestamp);
  }

  public static class Result {
    private final Level logLevel;
    private final LocalDateTime logTimestamp;

    public Result(String logLevel, LocalDateTime logTimestamp) {
      this.logLevel = LogLevel.resolveLevel(logLevel);
      this.logTimestamp = logTimestamp;
    }

    public Level getLogLevel() {
      return logLevel;
    }

    public LocalDateTime getLogTimestamp() {
      return logTimestamp;
    }

  }
}
