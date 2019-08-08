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
package org.apache.geode.logging.internal;

import static org.apache.geode.logging.internal.LogMessageRegex.Group.DATE;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.LOG_LEVEL;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.MEMBER_NAME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.MESSAGE;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.THREAD_ID;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.THREAD_NAME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.TIME;
import static org.apache.geode.logging.internal.LogMessageRegex.Group.TIME_ZONE;

import java.util.regex.Pattern;

/**
 * Regex with named capture groups that can be used to match Geode log messages.
 *
 * <p>
 * Example usage with AssertJ:
 *
 * <pre>
 * {@literal @}Test
 * public void logMessageMatches() {}
 *   String logMessage = "[info 2018/09/24 12:35:59.515 PDT logMessageRegexTest {@literal <}main> tid=0x1] this is a log statement";
 *   assertThat(logMessage).matches(LogMessageRegex.getRegex());
 * }
 * </pre>
 * <p>
 * Example usage with named group capturing:
 *
 * <pre>
 * {@literal @}Test
 * public void logLevelIsInfo() {
 *   Matcher matcher = getPattern().matcher(logMessage);
 *   assertThat(matcher.matches()).isTrue();
 *   assertThat(matcher.group(LogMessageRegex.Groups.LOG_LEVEL.getName())).isEqualTo("info");
 * }
 * </pre>
 * <p>
 * Note that a captured threadName will be in the form {@code <threadName>} and threadId will be in
 * the form {@code tid=hexThreadId} where hexThreadId is the threadId in hexadecimal.
 */
public class LogMessageRegex {

  private static final String REGEX = "^\\[" + LOG_LEVEL + " " + DATE + " " + TIME + " " + TIME_ZONE
      + " " + MEMBER_NAME + "[\\s]?" + THREAD_NAME + " " + THREAD_ID + "] " + MESSAGE + "$";

  public static String getRegex() {
    return REGEX;
  }

  public static Pattern getPattern() {
    return Pattern.compile(REGEX);
  }

  public enum Group {
    LOG_LEVEL(1, "logLevel", "(?<logLevel>[a-z]+)"),
    DATE(2, "date", "(?<date>\\d{4}\\/\\d{2}\\/\\d{2})"),
    TIME(3, "time", "(?<time>\\d{2}:\\d{2}:\\d{2}\\.\\d{3})"),
    TIME_ZONE(4, "timeZone", "(?<timeZone>[^ ]{3})"),
    MEMBER_NAME(5, "memberName", "(?:(?<memberName>(?:[0-9a-zA-Z]*(?:\\s[0-9a-zA-Z]+)?)*)?)"),
    THREAD_NAME(6, "threadName", "(?<threadName>\\<.+\\>)"),
    THREAD_ID(7, "threadId", "(?<threadId>tid=0[xX][0-9a-fA-F]+)"),
    MESSAGE(8, "message", "(?<message>.*)");

    private final int index;
    private final String name;
    private final String regex;

    Group(final int index, final String name, final String regex) {
      this.index = index;
      this.name = name;
      this.regex = regex;
    }

    public int getIndex() {
      return index;
    }

    public String getName() {
      return name;
    }

    public String getRegex() {
      return regex;
    }

    @Override
    public String toString() {
      return regex;
    }
  }
}
