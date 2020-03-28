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
package org.apache.geode.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static javax.management.JMX.newMXBeanProxy;
import static org.apache.geode.alerting.internal.spi.AlertLevel.NONE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.WARNING;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.management.JMXNotificationType.SYSTEM_ALERT;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedSystemName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Properties;

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
import org.mockito.ArgumentCaptor;

import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Integration tests for {@link DistributedSystemMXBean} with local {@link DistributedSystem}
 * connection.
 *
 * <p>
 * TODO:GEODE-5923: write more tests after fixing GEODE-5923
 */
@Category({ManagementTest.class, AlertingTest.class})
public class DistributedSystemMXBeanIntegrationTest {

  private static final Logger logger = LogService.getLogger();

  private static final long TIMEOUT = getTimeout().toMillis();
  private static final NotificationFilter SYSTEM_ALERT_FILTER =
      notification -> notification.getType().equals(SYSTEM_ALERT);

  private String name;
  private InternalCache cache;
  private DistributedMember distributedMember;
  private AlertingService alertingService;
  private NotificationListener notificationListener;
  private String alertMessage;

  private DistributedSystemMXBean distributedSystemMXBean;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    alertMessage = "Alerting in " + testName.getMethodName();
    name = "Manager in " + testName.getMethodName();

    String startLocator = getServerHostName() + "[" + getRandomAvailableTCPPort() + "]";

    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, name);
    config.setProperty(START_LOCATOR, startLocator);
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    distributedMember = cache.getDistributionManager().getId();
    alertingService = cache.getInternalDistributedSystem().getAlertingService();

    notificationListener = spy(NotificationListener.class);
    getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(),
        notificationListener, SYSTEM_ALERT_FILTER, null);

    distributedSystemMXBean = newMXBeanProxy(getPlatformMBeanServer(),
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

  @Test
  public void receivesLocalSystemAlertNotificationAtDefaultAlertLevel() {
    logger.fatal(alertMessage);

    assertThat(captureNotification().getMessage()).isEqualTo(alertMessage);
  }

  /**
   * Fails due to GEODE-5923: JMX manager receives local Alerts only for the default AlertLevel
   */
  @Test
  @Ignore("TODO:GEODE-5923: re-enable test after fixing GEODE-5923")
  public void receivesLocalSystemAlertNotificationAtNewAlertLevel() throws Exception {
    changeAlertLevel(WARNING);

    logger.warn(alertMessage);

    assertThat(captureNotification().getMessage()).isEqualTo(alertMessage);
  }

  private Notification captureNotification() {
    ArgumentCaptor<Notification> notificationCaptor = ArgumentCaptor.forClass(Notification.class);
    verify(notificationListener, timeout(TIMEOUT))
        .handleNotification(notificationCaptor.capture(), isNull());
    return notificationCaptor.getValue();
  }

  private void changeAlertLevel(AlertLevel alertLevel) throws Exception {
    distributedSystemMXBean.changeAlertLevel(alertLevel.name());

    if (alertLevel == NONE) {
      await().until(() -> !alertingService.hasAlertListener(distributedMember, alertLevel));
    } else {
      await().until(() -> alertingService.hasAlertListener(distributedMember, alertLevel));
    }
  }
}
