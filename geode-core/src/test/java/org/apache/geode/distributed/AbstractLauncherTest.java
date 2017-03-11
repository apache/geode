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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The AbstractLauncherTest class is a test suite of unit tests testing the contract and
 * functionality of the AbstractLauncher class.
 * <p/>
 * 
 * @see org.apache.geode.distributed.AbstractLauncher
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class AbstractLauncherTest {

  private AbstractLauncher<?> createAbstractLauncher(final String memberName,
      final String memberId) {
    return new FakeServiceLauncher(memberName, memberId);
  }

  @Test
  public void testIsSet() {
    final Properties properties = new Properties();

    assertFalse(properties.containsKey(NAME));
    assertFalse(AbstractLauncher.isSet(properties, NAME));

    properties.setProperty(NAME, "");

    assertTrue(properties.containsKey(NAME));
    assertFalse(AbstractLauncher.isSet(properties, NAME));

    properties.setProperty(NAME, "  ");

    assertTrue(properties.containsKey(NAME));
    assertFalse(AbstractLauncher.isSet(properties, NAME));

    properties.setProperty(NAME, "memberOne");

    assertTrue(AbstractLauncher.isSet(properties, NAME));
    assertFalse(AbstractLauncher.isSet(properties, "NaMe"));
  }

  @Test
  public void testLoadGemFirePropertiesWithNullURL() {
    final Properties properties = AbstractLauncher.loadGemFireProperties(null);
    assertNotNull(properties);
    assertTrue(properties.isEmpty());
  }

  @Test
  public void testLoadGemFirePropertiesWithNonExistingURL() throws MalformedURLException {
    final Properties properties = AbstractLauncher
        .loadGemFireProperties(new URL("file:///path/to/non_existing/gemfire.properties"));
    assertNotNull(properties);
    assertTrue(properties.isEmpty());
  }

  @Test
  public void testGetDistributedSystemProperties() {
    AbstractLauncher<?> launcher = createAbstractLauncher("memberOne", "1");

    assertNotNull(launcher);
    assertEquals("1", launcher.getMemberId());
    assertEquals("memberOne", launcher.getMemberName());

    Properties distributedSystemProperties = launcher.getDistributedSystemProperties();

    assertNotNull(distributedSystemProperties);
    assertTrue(distributedSystemProperties.containsKey(NAME));
    assertEquals("memberOne", distributedSystemProperties.getProperty(NAME));

    launcher = createAbstractLauncher(null, "22");

    assertNotNull(launcher);
    assertEquals("22", launcher.getMemberId());
    assertNull(launcher.getMemberName());

    distributedSystemProperties = launcher.getDistributedSystemProperties();

    assertNotNull(distributedSystemProperties);
    assertFalse(distributedSystemProperties.containsKey(NAME));

    launcher = createAbstractLauncher(StringUtils.EMPTY_STRING, "333");

    assertNotNull(launcher);
    assertEquals("333", launcher.getMemberId());
    assertEquals(StringUtils.EMPTY_STRING, launcher.getMemberName());

    distributedSystemProperties = launcher.getDistributedSystemProperties();

    assertNotNull(distributedSystemProperties);
    assertFalse(distributedSystemProperties.containsKey(NAME));

    launcher = createAbstractLauncher("  ", "4444");

    assertNotNull(launcher);
    assertEquals("4444", launcher.getMemberId());
    assertEquals("  ", launcher.getMemberName());

    distributedSystemProperties = launcher.getDistributedSystemProperties();

    assertNotNull(distributedSystemProperties);
    assertFalse(distributedSystemProperties.containsKey(NAME));
  }

  @Test
  public void testGetDistributedSystemPropertiesWithDefaults() {
    AbstractLauncher<?> launcher = createAbstractLauncher("TestMember", "123");

    assertNotNull(launcher);
    assertEquals("123", launcher.getMemberId());
    assertEquals("TestMember", launcher.getMemberName());

    Properties defaults = new Properties();

    defaults.setProperty("testKey", "testValue");

    Properties distributedSystemProperties = launcher.getDistributedSystemProperties(defaults);

    assertNotNull(distributedSystemProperties);
    assertEquals(launcher.getMemberName(), distributedSystemProperties.getProperty(NAME));
    assertEquals("testValue", distributedSystemProperties.getProperty("testKey"));
  }

  @Test
  public void testGetMember() {
    AbstractLauncher<?> launcher = createAbstractLauncher("memberOne", "123");

    assertNotNull(launcher);
    assertEquals("123", launcher.getMemberId());
    assertEquals("memberOne", launcher.getMemberName());
    assertEquals("memberOne", launcher.getMember());

    launcher = createAbstractLauncher(null, "123");

    assertNotNull(launcher);
    assertEquals("123", launcher.getMemberId());
    assertNull(launcher.getMemberName());
    assertEquals("123", launcher.getMember());

    launcher = createAbstractLauncher(StringUtils.EMPTY_STRING, "123");

    assertNotNull(launcher);
    assertEquals("123", launcher.getMemberId());
    assertEquals(StringUtils.EMPTY_STRING, launcher.getMemberName());
    assertEquals("123", launcher.getMember());

    launcher = createAbstractLauncher(" ", "123");

    assertNotNull(launcher);
    assertEquals("123", launcher.getMemberId());
    assertEquals(" ", launcher.getMemberName());
    assertEquals("123", launcher.getMember());

    launcher = createAbstractLauncher(null, StringUtils.EMPTY_STRING);

    assertNotNull(launcher);
    assertEquals(StringUtils.EMPTY_STRING, launcher.getMemberId());
    assertNull(launcher.getMemberName());
    assertNull(launcher.getMember());

    launcher = createAbstractLauncher(null, " ");

    assertNotNull(launcher);
    assertEquals(" ", launcher.getMemberId());
    assertNull(launcher.getMemberName());
    assertNull(launcher.getMember());
  }

  @Test
  public void testAbstractLauncherServiceStateToDaysHoursMinutesSeconds() {
    assertEquals("", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(null));
    assertEquals("0 seconds", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(0l));
    assertEquals("1 second", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(1000l));
    assertEquals("1 second", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(1999l));
    assertEquals("2 seconds", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(2001l));
    assertEquals("45 seconds", AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(45000l));
    assertEquals("1 minute 0 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 1000l));
    assertEquals("1 minute 1 second",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(61 * 1000l));
    assertEquals("1 minute 30 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(90 * 1000l));
    assertEquals("2 minutes 0 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(120 * 1000l));
    assertEquals("2 minutes 1 second",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(121 * 1000l));
    assertEquals("2 minutes 15 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(135 * 1000l));
    assertEquals("1 hour 0 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 60 * 1000l));
    assertEquals("1 hour 1 second",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 60 * 1000l + 1000l));
    assertEquals("1 hour 15 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 60 * 1000l + 15000l));
    assertEquals("1 hour 1 minute 0 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 61 * 1000l));
    assertEquals("1 hour 1 minute 1 second",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 61 * 1000l + 1000l));
    assertEquals("1 hour 1 minute 45 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 61 * 1000l + 45000l));
    assertEquals("1 hour 2 minutes 0 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 62 * 1000l));
    assertEquals("1 hour 5 minutes 1 second",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 65 * 1000l + 1000l));
    assertEquals("1 hour 5 minutes 10 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 65 * 1000l + 10000l));
    assertEquals("1 hour 59 minutes 11 seconds",
        AbstractLauncher.ServiceState.toDaysHoursMinutesSeconds(60 * 119 * 1000l + 11000l));
    assertEquals("1 day 1 hour 1 minute 1 second",
        AbstractLauncher.ServiceState
            .toDaysHoursMinutesSeconds(TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(1)
                + TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
    assertEquals("1 day 5 hours 15 minutes 45 seconds",
        AbstractLauncher.ServiceState
            .toDaysHoursMinutesSeconds(TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(5)
                + TimeUnit.MINUTES.toMillis(15) + TimeUnit.SECONDS.toMillis(45)));
    assertEquals("2 days 1 hour 30 minutes 1 second",
        AbstractLauncher.ServiceState
            .toDaysHoursMinutesSeconds(TimeUnit.DAYS.toMillis(2) + TimeUnit.HOURS.toMillis(1)
                + TimeUnit.MINUTES.toMillis(30) + TimeUnit.SECONDS.toMillis(1)));
  }

  private static final class FakeServiceLauncher extends AbstractLauncher<String> {

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
