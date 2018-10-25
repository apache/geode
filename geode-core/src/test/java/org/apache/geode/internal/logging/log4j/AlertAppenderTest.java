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
package org.apache.geode.internal.logging.log4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link AlertAppender}.
 */
@Category(LoggingTest.class)
public class AlertAppenderTest {

  private List<DistributedMember> members;
  private Level previousLogLevel;

  @Before
  public void setUp() {
    members = new ArrayList<>();
    previousLogLevel = LogService.getBaseLogLevel();
  }

  @After
  public void tearDown() {
    LogService.setBaseLogLevel(previousLogLevel);
    if (!members.isEmpty()) {
      for (DistributedMember member : members) {
        AlertAppender.getInstance().removeAlertListener(member);
      }
      members.clear();
    }
  }

  /**
   * Verify that adding/removing/replacing listeners works correctly.
   */
  @Test
  public void testListenerHandling() throws Exception {
    DistributedMember member1 = createNamedDistributedMember("Member1");
    DistributedMember member2 = createNamedDistributedMember("Member2");
    DistributedMember member3 = createNamedDistributedMember("Member3");
    DistributedMember member4 = createNamedDistributedMember("Member4");
    DistributedMember member5 = createNamedDistributedMember("Member5");
    DistributedMember member6 = createNamedDistributedMember("Member6");

    LogService.setBaseLogLevel(Level.WARN);

    AlertAppender.getInstance().addAlertListener(member1, Alert.SEVERE);
    AlertAppender.getInstance().addAlertListener(member2, Alert.WARNING);
    AlertAppender.getInstance().addAlertListener(member3, Alert.ERROR);
    AlertAppender.getInstance().addAlertListener(member4, Alert.ERROR);
    AlertAppender.getInstance().addAlertListener(member5, Alert.WARNING);
    AlertAppender.getInstance().addAlertListener(member6, Alert.SEVERE);

    Field listenersField = AlertAppender.getInstance().getClass().getDeclaredField("listeners");
    listenersField.setAccessible(true);

    CopyOnWriteArrayList<AlertAppender.Listener> listeners =
        (CopyOnWriteArrayList<AlertAppender.Listener>) listenersField
            .get(AlertAppender.getInstance());

    // Verify add
    assertSame(member5, listeners.get(0).getMember());
    assertSame(member2, listeners.get(1).getMember());
    assertSame(member4, listeners.get(2).getMember());
    assertSame(member3, listeners.get(3).getMember());
    assertSame(member6, listeners.get(4).getMember());
    assertSame(member1, listeners.get(5).getMember());
    assertSame(6, listeners.size());

    // Verify replace with same level
    AlertAppender.getInstance().addAlertListener(member5, Alert.WARNING);
    assertSame(member5, listeners.get(0).getMember());
    assertSame(member2, listeners.get(1).getMember());
    assertSame(member4, listeners.get(2).getMember());
    assertSame(member3, listeners.get(3).getMember());
    assertSame(member6, listeners.get(4).getMember());
    assertSame(member1, listeners.get(5).getMember());
    assertSame(6, listeners.size());

    // Verify replace with difference level
    AlertAppender.getInstance().addAlertListener(member5, Alert.SEVERE);
    assertSame(member2, listeners.get(0).getMember());
    assertSame(member4, listeners.get(1).getMember());
    assertSame(member3, listeners.get(2).getMember());
    assertSame(member5, listeners.get(3).getMember());
    assertSame(member6, listeners.get(4).getMember());
    assertSame(member1, listeners.get(5).getMember());
    assertSame(6, listeners.size());

    // Verify remove
    assertTrue(AlertAppender.getInstance().removeAlertListener(member3));
    assertSame(member2, listeners.get(0).getMember());
    assertSame(member4, listeners.get(1).getMember());
    assertSame(member5, listeners.get(2).getMember());
    assertSame(member6, listeners.get(3).getMember());
    assertSame(member1, listeners.get(4).getMember());
    assertSame(5, listeners.size());

    assertTrue(AlertAppender.getInstance().removeAlertListener(member1));
    assertTrue(AlertAppender.getInstance().removeAlertListener(member2));
    assertFalse(AlertAppender.getInstance().removeAlertListener(member3));
    assertTrue(AlertAppender.getInstance().removeAlertListener(member4));
    assertTrue(AlertAppender.getInstance().removeAlertListener(member5));
    assertTrue(AlertAppender.getInstance().removeAlertListener(member6));
  }

  /**
   * Verifies that the appender is correctly added and removed from the Log4j configuration and that
   * when the configuration is changed the appender is still there.
   */
  @Test
  public void testAppenderToConfigHandling() {
    LogService.setBaseLogLevel(Level.WARN);

    String appenderName = AlertAppender.getInstance().getName();
    AppenderContext appenderContext = LogService.getAppenderContext();

    LoggerConfig loggerConfig = appenderContext.getLoggerConfig();

    // Find out home many appenders exist before we get started
    int startingSize = loggerConfig.getAppenders().size();

    // Add a listener and verify that the appender was added to log4j
    DistributedMember member1 = createNamedDistributedMember("Member1");
    AlertAppender.getInstance().addAlertListener(member1, Alert.SEVERE);
    assertEquals(loggerConfig.getAppenders().values().toString(), startingSize + 1,
        loggerConfig.getAppenders().size());
    assertTrue(loggerConfig.getAppenders().containsKey(appenderName));

    // Add another listener and verify that there's still only 1 alert appender
    DistributedMember member2 = createNamedDistributedMember("Member1");
    AlertAppender.getInstance().addAlertListener(member2, Alert.SEVERE);
    assertEquals(startingSize + 1, loggerConfig.getAppenders().size());

    // Modify the config and verify that the appender still exists
    assertEquals(Level.WARN, LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel());

    LogService.setBaseLogLevel(Level.INFO);

    assertEquals(Level.INFO, LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel());
    loggerConfig = appenderContext.getLoggerConfig();
    assertEquals(startingSize + 1, loggerConfig.getAppenders().size());
    assertTrue(loggerConfig.getAppenders().containsKey(appenderName));

    // Remove the listeners and verify that the appender was removed from log4j
    assertTrue(AlertAppender.getInstance().removeAlertListener(member2));
    assertFalse(AlertAppender.getInstance().removeAlertListener(member1));
    assertEquals(startingSize, loggerConfig.getAppenders().size());
    assertFalse(loggerConfig.getAppenders().containsKey(appenderName));
  }

  private DistributedMember createNamedDistributedMember(final String name) {
    return new NamedDistributedMember(name);
  }

  private static class NamedDistributedMember implements DistributedMember {

    private final String name;

    NamedDistributedMember(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getHost() {
      return "";
    }

    @Override
    public Set<Role> getRoles() {
      return null;
    }

    @Override
    public int getProcessId() {
      return 0;
    }

    @Override
    public String getId() {
      return name;
    }

    @Override
    public int compareTo(final DistributedMember o) {
      return getName().compareTo(o.getName());
    }

    @Override
    public DurableClientAttributes getDurableClientAttributes() {
      return null;
    }

    @Override
    public List<String> getGroups() {
      return Collections.emptyList();
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof NamedDistributedMember)) {
        return false;
      }
      NamedDistributedMember other = (NamedDistributedMember) obj;
      return getName().equals(other.getName());
    }

    @Override
    public int hashCode() {
      return getHost().hashCode();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [name=" + name + "]";
    }
  }
}
