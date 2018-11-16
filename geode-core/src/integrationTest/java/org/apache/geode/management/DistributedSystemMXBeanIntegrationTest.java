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
package org.apache.geode.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedSystemName;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import javax.management.JMX;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Integration tests for {@link DistributedSystemMXBean} with local {@link DistributedSystem}
 * connection.
 */
@Category(ManagementTest.class)
public class DistributedSystemMXBeanIntegrationTest {

  private String name;
  private InternalCache cache;
  private Logger logger;
  private String alertMessage;

  private DistributedSystemMXBean distributedSystemMXBean;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    name = "Manager in " + testName.getMethodName();

    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, name);

    config.setProperty(LOCATORS, "");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");
    config.setProperty(ENABLE_TIME_STATISTICS, "true");
    config.setProperty(STATISTIC_SAMPLING_ENABLED, "true");

    cache = (InternalCache) new CacheFactory(config).create();
    logger = LogService.getLogger();

    alertMessage = "Alerting in " + testName.getMethodName();

    distributedSystemMXBean = JMX.newMXBeanProxy(getPlatformMBeanServer(),
        getDistributedSystemName(), DistributedSystemMXBean.class);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void isRegisteredInManager() {
    assertThat(getPlatformMBeanServer().isRegistered(getDistributedSystemName())).isTrue();
  }

  @Test
  public void supportsJmxProxy() {
    assertThat(distributedSystemMXBean).isNotNull();
    assertThat(distributedSystemMXBean.listMembers()).containsExactly(name);
  }

  /**
   * This test confirms existence of bug GEODE-5923.
   */
  @Test
  @Ignore("GEODE-5923")
  public void providesSystemAlertNotification() throws Exception {
    NotificationListener notificationListener = spy(NotificationListener.class);
    NotificationFilter notificationFilter = (Notification notification) -> notification.getType()
        .equals(JMXNotificationType.SYSTEM_ALERT);
    getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(),
        notificationListener, notificationFilter, null);

    // work around GEODE-5924 by invoking DistributedSystemMXBean.changeAlertLevel
    distributedSystemMXBean.changeAlertLevel("warning");

    logger.fatal(alertMessage);

    verify(notificationListener).handleNotification(isA(Notification.class), isNull());
  }
}
