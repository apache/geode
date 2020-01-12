/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import static org.apache.geode.logging.internal.LogMessageRegex.Group.values;
import static org.apache.geode.logging.internal.LogMessageRegex.getPattern;
import static org.apache.geode.logging.internal.LogMessageRegex.getRegex;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogMessageRegex} matching startup configuration with optional
 * {@code memberName} missing.
 *
 * <p>
 * Example log message containing startup configuration:
 *
 * <pre>
 * [info 2018/10/19 16:03:18.069 PDT <main> tid=0x1] Startup Configuration: ### GemFire Properties defined with api ###
 * </pre>
 */
@Category(LoggingTest.class)
public class LogMessageRegexMatchesStartupConfigurationTest {

  private final String logLevel = "info";
  private final String date = "2018/10/19";
  private final String time = "16:03:18.069";
  private final String timeZone = "PDT";
  private final String memberName = "";
  private final String threadName = "<main>";
  private final String threadId = "tid=0x1";
  private final String message =
      "Startup Configuration: ### GemFire Properties defined with api ###";

  private String logLine;

  @Before
  public void setUp() {
    logLine = "[" + logLevel + " " + date + " " + time + " " + timeZone + " " + memberName
        + threadName + " " + threadId + "] " + message;
  }

  @Test
  public void regexMatchesStartupConfigurationLogLine() {
    assertThat(logLine).matches(getRegex());
  }

  @Test
  public void patternMatcherMatchesStartupConfigurationLogLine() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void patternMatcherGroupZeroMatchesStartupConfigurationLogLine() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(0)).isEqualTo(logLine);
  }

  @Test
  public void patternMatcherGroupCountEqualsGroupsLength() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.groupCount()).isEqualTo(values().length);
  }

  @Test
  public void logLevelGroupIndexCapturesStartupConfigurationLogLevel() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getIndex())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupIndexCapturesStartupConfigurationDate() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getIndex())).isEqualTo(date);
  }

  @Test
  public void timeGroupIndexCapturesStartupConfigurationTime() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getIndex())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupIndexCapturesStartupConfigurationTimeZone() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getIndex())).isEqualTo(timeZone);
  }

  @Test
  public void logLevelGroupNameCapturesStartupConfigurationLogLevel() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(LOG_LEVEL.getName())).isEqualTo(logLevel);
  }

  @Test
  public void dateGroupNameCapturesStartupConfigurationDate() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(DATE.getName())).isEqualTo(date);
  }

  @Test
  public void timeGroupNameCapturesStartupConfigurationTime() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME.getName())).isEqualTo(time);
  }

  @Test
  public void timeZoneGroupNameCapturesStartupConfigurationTimeZone() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(TIME_ZONE.getName())).isEqualTo(timeZone);
  }

  @Test
  public void memberNameGroupNameCapturesStartupConfigurationMemberName() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MEMBER_NAME.getName())).isEqualTo(memberName);
  }

  @Test
  public void threadNameGroupNameCapturesStartupConfigurationThreadName() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_NAME.getName())).isEqualTo(threadName);
  }

  @Test
  public void threadIdGroupNameCapturesStartupConfigurationThreadId() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(THREAD_ID.getName())).isEqualTo(threadId);
  }

  @Test
  public void messageGroupNameCapturesStartupConfigurationMessage() {
    Matcher matcher = getPattern().matcher(logLine);
    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(MESSAGE.getName())).isEqualTo(message);
  }

  @Test
  public void logLevelRegexMatchesStartupConfigurationLogLevel() {
    Matcher matcher = Pattern.compile(LOG_LEVEL.getRegex()).matcher(logLevel);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void dateRegexMatchesStartupConfigurationDate() {
    Matcher matcher = Pattern.compile(DATE.getRegex()).matcher(date);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeRegexMatchesStartupConfigurationTime() {
    Matcher matcher = Pattern.compile(TIME.getRegex()).matcher(time);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void timeZoneRegexMatchesStartupConfigurationTimeZone() {
    Matcher matcher = Pattern.compile(TIME_ZONE.getRegex()).matcher(timeZone);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void memberNameRegexMatchesStartupConfigurationMemberName() {
    Matcher matcher = Pattern.compile(MEMBER_NAME.getRegex()).matcher(memberName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadNameRegexMatchesStartupConfigurationThreadName() {
    Matcher matcher = Pattern.compile(THREAD_NAME.getRegex()).matcher(threadName);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void threadIdRegexMatchesStartupConfigurationThreadId() {
    Matcher matcher = Pattern.compile(THREAD_ID.getRegex()).matcher(threadId);
    assertThat(matcher.matches()).isTrue();
  }

  @Test
  public void messageRegexMatchesStartupConfigurationMessage() {
    Matcher matcher = Pattern.compile(MESSAGE.getRegex()).matcher(message);
    assertThat(matcher.matches()).isTrue();
  }
}
