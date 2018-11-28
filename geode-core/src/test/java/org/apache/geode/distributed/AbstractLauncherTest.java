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
package org.apache.geode.distributed;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URL;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;


/**
 * Unit tests for {@link AbstractLauncher}.
 *
 * @since GemFire 7.0
 */
public class AbstractLauncherTest {

  @Test
  public void canBeMocked() throws Exception {
    AbstractLauncher mockAbstractLauncher = mock(AbstractLauncher.class);
    mockAbstractLauncher.setDebug(true);
    verify(mockAbstractLauncher, times(1)).setDebug(true);
  }

  @Test
  public void isSetReturnsFalseIfPropertyDoesNotExist() throws Exception {
    assertThat(AbstractLauncher.isSet(new Properties(), NAME)).isFalse();
  }

  @Test
  public void isSetReturnsFalseIfPropertyHasEmptyValue() throws Exception {
    Properties properties = new Properties();

    properties.setProperty(NAME, "");

    assertThat(AbstractLauncher.isSet(properties, NAME)).isFalse();
  }

  @Test
  public void isSetReturnsFalseIfPropertyHasBlankValue() throws Exception {
    Properties properties = new Properties();

    properties.setProperty(NAME, "  ");

    assertThat(AbstractLauncher.isSet(properties, NAME)).isFalse();
  }

  @Test
  public void isSetReturnsTrueIfPropertyHasRealValue() throws Exception {
    Properties properties = new Properties();

    properties.setProperty(NAME, "memberOne");

    assertThat(AbstractLauncher.isSet(properties, NAME)).isTrue();
  }

  @Test
  public void isSetKeyIsCaseSensitive() throws Exception {
    Properties properties = new Properties();

    properties.setProperty(NAME, "memberOne");

    assertThat(AbstractLauncher.isSet(properties, "NaMe")).isFalse();
  }

  @Test
  public void loadGemFirePropertiesWithNullURLReturnsEmptyProperties() throws Exception {
    URL nullUrl = null;

    Properties properties = AbstractLauncher.loadGemFireProperties(nullUrl);

    assertThat(properties).isNotNull().isEmpty();
  }

  @Test
  public void loadGemFirePropertiesWithNonExistingURLReturnsEmptyProperties() throws Exception {
    URL nonExistingUrl = new URL("file:///path/to/non_existing/gemfire.properties");

    Properties properties = AbstractLauncher.loadGemFireProperties(nonExistingUrl);

    assertThat(properties).isNotNull().isEmpty();
  }

  @Test
  public void getDistributedSystemPropertiesContainsMemberNameAsName() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("memberOne", "1");

    Properties properties = launcher.getDistributedSystemProperties();

    assertThat(properties).containsExactly(entry(NAME, "memberOne"));
  }

  @Test
  public void getDistributedSystemPropertiesIsEmptyWhenMemberNameIsNull() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, "22");

    Properties properties = launcher.getDistributedSystemProperties();

    assertThat(properties).isEmpty();
  }

  @Test
  public void getDistributedSystemPropertiesIsEmptyWhenMemberNameIsEmpty() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(StringUtils.EMPTY, "333");

    Properties properties = launcher.getDistributedSystemProperties();

    assertThat(properties).isEmpty();
  }

  @Test
  public void getDistributedSystemPropertiesIsEmptyWhenMemberNameIsBlank() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("  ", "4444");

    Properties properties = launcher.getDistributedSystemProperties();

    assertThat(properties).isEmpty();
  }

  @Test
  public void getDistributedSystemPropertiesIncludesDefaults() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("TestMember", "123");
    Properties defaults = new Properties();
    defaults.setProperty("testKey", "testValue");

    Properties properties = launcher.getDistributedSystemProperties(defaults);

    assertThat(properties.getProperty(NAME)).isEqualTo(launcher.getMemberName());
    assertThat(properties.getProperty("testKey")).isEqualTo("testValue");
  }

  @Test
  public void getMemberNameReturnsValue() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("memberOne", null);

    assertThat(launcher.getMemberName()).isEqualTo("memberOne");
  }

  @Test
  public void getMemberNameReturnsEmptyIfEmpty() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(StringUtils.EMPTY, null);

    assertThat(launcher.getMemberName()).isEqualTo(StringUtils.EMPTY);
  }

  @Test
  public void getMemberNameReturnsBlankIfBlank() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(" ", null);

    assertThat(launcher.getMemberName()).isEqualTo(" ");
  }

  @Test
  public void getMemberNameReturnsSameNumberOfBlanks() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("   ", null);

    assertThat(launcher.getMemberName()).isEqualTo("   ");
  }

  @Test
  public void getMemberIdReturnsValue() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, "123");

    assertThat(launcher.getMemberId()).isEqualTo("123");
  }

  @Test
  public void getMemberIdReturnsNullIfNull() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, null);

    assertThat(launcher.getMemberId()).isNull();
  }

  @Test
  public void getMemberIdReturnsEmptyIfEmpty() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, StringUtils.EMPTY);

    assertThat(launcher.getMemberId()).isEqualTo(StringUtils.EMPTY);
  }

  @Test
  public void getMemberIdReturnsBlankIfBlank() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, " ");

    assertThat(launcher.getMemberId()).isEqualTo(" ");
  }

  @Test
  public void getMemberIdReturnsSameNumberOfBlanks() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, "   ");

    assertThat(launcher.getMemberId()).isEqualTo("   ");
  }

  @Test
  public void getMemberPrefersMemberNameOverMemberId() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher("memberOne", "123");

    assertThat(launcher.getMember()).isEqualTo("memberOne").isEqualTo(launcher.getMemberName());
  }

  @Test
  public void getMemberReturnsMemberIdIfMemberNameIsNull() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, "123");

    assertThat(launcher.getMember()).isEqualTo("123").isEqualTo(launcher.getMemberId());
  }

  @Test
  public void getMemberReturnsMemberIdIfMemberNameIsEmpty() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(StringUtils.EMPTY, "123");

    assertThat(launcher.getMember()).isEqualTo("123").isEqualTo(launcher.getMemberId());
  }

  @Test
  public void getMemberReturnsMemberIdIfMemberNameIsBlank() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(" ", "123");

    assertThat(launcher.getMember()).isEqualTo("123").isEqualTo(launcher.getMemberId());
  }

  @Test
  public void getMemberReturnsNullIfMemberIdIsNull() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, null);

    assertThat(launcher.getMember()).isNull();
  }

  @Test
  public void getMemberReturnNullIfMemberIdIsEmpty() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, StringUtils.EMPTY);

    assertThat(launcher.getMember()).isNull();
  }

  @Test
  public void getMemberReturnNullIfMemberIdIsBlank() throws Exception {
    AbstractLauncher<?> launcher = createAbstractLauncher(null, " ");

    assertThat(launcher.getMember()).isNull();
  }

  @Test
  public void toDaysHoursMinutesSeconds_null_returnsEmptyString() throws Exception {
    assertThat(toDaysHoursMinutesSeconds(null)).isEqualTo("");
  }

  @Test
  public void toDaysHoursMinutesSeconds_milliseconds_returnsSecondsString() throws Exception {
    assertThat(toDaysHoursMinutesSeconds(MILLISECONDS.toMillis(0))).isEqualTo("0 seconds");
    assertThat(toDaysHoursMinutesSeconds(MILLISECONDS.toMillis(999))).isEqualTo("0 seconds");
    assertThat(toDaysHoursMinutesSeconds(MILLISECONDS.toMillis(1000))).isEqualTo("1 second");
    assertThat(toDaysHoursMinutesSeconds(MILLISECONDS.toMillis(1999))).isEqualTo("1 second");
  }

  @Test
  public void toDaysHoursMinutesSeconds_seconds_returnsSecondsString() throws Exception {
    assertThat(toDaysHoursMinutesSeconds(SECONDS.toMillis(0))).isEqualTo("0 seconds");
    assertThat(toDaysHoursMinutesSeconds(SECONDS.toMillis(1))).isEqualTo("1 second");
    assertThat(toDaysHoursMinutesSeconds(SECONDS.toMillis(2))).isEqualTo("2 seconds");
    assertThat(toDaysHoursMinutesSeconds(SECONDS.toMillis(45))).isEqualTo("45 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_minutes_returnsMinutesAndSecondsString() throws Exception {
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(1))).isEqualTo("1 minute 0 seconds");
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(2))).isEqualTo("2 minutes 0 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_minutesAndSeconds_returnsMinutesAndSecondsString()
      throws Exception {
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(1) + SECONDS.toMillis(1)))
        .isEqualTo("1 minute 1 second");
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(1) + SECONDS.toMillis(30)))
        .isEqualTo("1 minute 30 seconds");
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(2) + SECONDS.toMillis(1)))
        .isEqualTo("2 minutes 1 second");
    assertThat(toDaysHoursMinutesSeconds(MINUTES.toMillis(2) + SECONDS.toMillis(15)))
        .isEqualTo("2 minutes 15 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_hours_returnsHoursAndSecondsString() throws Exception {
    assertThat(toDaysHoursMinutesSeconds(HOURS.toMillis(1))).isEqualTo("1 hour 0 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_hoursAndSeconds_returnsHoursAndSecondsString()
      throws Exception {
    assertThat(toDaysHoursMinutesSeconds(HOURS.toMillis(1) + SECONDS.toMillis(1)))
        .isEqualTo("1 hour 1 second");
    assertThat(toDaysHoursMinutesSeconds(HOURS.toMillis(1) + SECONDS.toMillis(15)))
        .isEqualTo("1 hour 15 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_hoursAndMinutes_returnsHoursAndMinutesAndSecondsString()
      throws Exception {
    assertThat(toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(1)))
        .isEqualTo("1 hour 1 minute 0 seconds");
    assertThat(toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(2)))
        .isEqualTo("1 hour 2 minutes 0 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_hoursAndMinutesAndSeconds_returnsHoursAndMinutesAndSecondsString()
      throws Exception {
    assertThat(
        toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(1) + SECONDS.toMillis(1)))
            .isEqualTo("1 hour 1 minute 1 second");
    assertThat(
        toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(1) + SECONDS.toMillis(45)))
            .isEqualTo("1 hour 1 minute 45 seconds");
    assertThat(
        toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(5) + SECONDS.toMillis(1)))
            .isEqualTo("1 hour 5 minutes 1 second");
    assertThat(
        toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(5) + SECONDS.toMillis(10)))
            .isEqualTo("1 hour 5 minutes 10 seconds");
    assertThat(
        toDaysHoursMinutesSeconds(HOURS.toMillis(1) + MINUTES.toMillis(59) + SECONDS.toMillis(11)))
            .isEqualTo("1 hour 59 minutes 11 seconds");
  }

  @Test
  public void toDaysHoursMinutesSeconds_daysAndHoursAndMinutesAndSeconds_returnsDaysAndHoursAndMinutesAndSecondsString()
      throws Exception {
    assertThat(toDaysHoursMinutesSeconds(
        DAYS.toMillis(1) + HOURS.toMillis(1) + MINUTES.toMillis(1) + SECONDS.toMillis(1)))
            .isEqualTo("1 day 1 hour 1 minute 1 second");
    assertThat(toDaysHoursMinutesSeconds(
        DAYS.toMillis(1) + HOURS.toMillis(5) + MINUTES.toMillis(15) + SECONDS.toMillis(45)))
            .isEqualTo("1 day 5 hours 15 minutes 45 seconds");
    assertThat(toDaysHoursMinutesSeconds(
        DAYS.toMillis(2) + HOURS.toMillis(1) + MINUTES.toMillis(30) + SECONDS.toMillis(1)))
            .isEqualTo("2 days 1 hour 30 minutes 1 second");
  }

  private AbstractLauncher<?> createAbstractLauncher(final String memberName,
      final String memberId) {
    return new FakeServiceLauncher(memberName, memberId);
  }

  private static class FakeServiceLauncher extends AbstractLauncher<String> {

    private final String memberId;
    private final String memberName;

    public FakeServiceLauncher(final String memberName, final String memberId) {
      this.memberId = memberId;
      this.memberName = memberName;
    }

    @Override
    public String getLogFileName() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    @Override
    public String getMemberId() {
      return memberId;
    }

    @Override
    public String getMemberName() {
      return memberName;
    }

    @Override
    public Integer getPid() {
      throw new UnsupportedOperationException("Not Implemented!");
    }

    @Override
    public String getServiceName() {
      return "TestService";
    }

    @Override
    public void run() {
      throw new UnsupportedOperationException("Not Implemented!");
    }
  }
}
