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
package org.apache.geode.internal.alerting;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.addListener;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.removeListener;
import static org.apache.geode.internal.alerting.AlertLevel.NONE;
import static org.apache.geode.internal.alerting.AlertLevel.SEVERE;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedSystemName;
import static org.apache.geode.management.internal.MBeanJMXAdapter.mbeanServer;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.Serializable;
import java.util.Properties;

import javax.management.JMX;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.ManagementTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for {@link AlertingService} with {@link DistributedSystemMXBean} in the
 * JMX Manager without any {@code NotificationListener}s.
 */
@Category({AlertingTest.class, ManagementTest.class})
public class AlertingServiceWithoutListenerDistributedTest implements Serializable {

  private static InternalCache cache;
  private static Logger logger;

  private static AlertListenerMessage.Listener messageListener;
  private static DistributedSystemMXBean distributedSystemMXBean;
  private static AlertingService alertingService;

  private DistributedMember managerMember;

  private String alertMessage;

  private String managerName;
  private String memberName;

  private VM managerVM;
  private VM memberVM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    alertMessage = "Alerting in " + testName.getMethodName();

    managerName = "Manager in " + testName.getMethodName();
    memberName = "Member in " + testName.getMethodName();

    managerVM = getVM(0);
    memberVM = getController();

    managerMember = managerVM.invoke(() -> createManager());
    memberVM.invoke(() -> createMember());

    addIgnoredException(alertMessage);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(managerVM, memberVM)) {
      vm.invoke(() -> {
        removeListener(messageListener);
        cache.close();
        cache = null;
        logger = null;
        messageListener = null;
        distributedSystemMXBean = null;
        alertingService = null;
      });
    }
  }

  @Test
  public void distributedSystemMXBeanExists() {
    managerVM.invoke(() -> {
      assertThat(distributedSystemMXBean).isNotNull();
    });
  }

  @Test
  public void distributedSystemMXBeanIsRegistered() {
    managerVM.invoke(() -> {
      assertThat(mbeanServer.isRegistered(getDistributedSystemName())).isTrue();
    });
  }

  @Test
  public void alertListenerIsRegisteredForManager() {
    memberVM.invoke(() -> {
      assertThat(alertingService.hasAlertListener(managerMember, SEVERE)).isTrue();
    });
  }

  @Test
  public void alertMessageIsReceivedByManager() {
    memberVM.invoke(() -> logger.fatal(alertMessage));

    managerVM.invoke(() -> verifyNoMoreInteractions(messageListener));
  }

  @Test
  public void alertListenerMessageIsNotReceivedByManagerForLevelNone() {
    changeAlertLevel(NONE);

    memberVM.invoke(() -> logger.fatal(alertMessage));

    managerVM.invoke(() -> verifyNoMoreInteractions(messageListener));
  }

  @Test
  public void alertListenerMessageIsNotReceivedByManagerForLevelsLowerThanAlertLevel() {
    memberVM.invoke(() -> {
      logger.warn(alertMessage);
      logger.error(alertMessage);
    });

    managerVM.invoke(() -> verifyNoMoreInteractions(messageListener));
  }

  private DistributedMember createManager() {
    messageListener = spy(AlertListenerMessage.Listener.class);
    addListener(messageListener);

    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, managerName);
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    alertingService = cache.getInternalDistributedSystem().getAlertingService();
    logger = LogService.getLogger();

    distributedSystemMXBean = JMX.newMXBeanProxy(getPlatformMBeanServer(),
        getDistributedSystemName(), DistributedSystemMXBean.class);

    return cache.getDistributedSystem().getDistributedMember();
  }

  private void createMember() {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, memberName);
    config.setProperty(JMX_MANAGER, "false");

    cache = (InternalCache) new CacheFactory(config).create();
    alertingService = cache.getInternalDistributedSystem().getAlertingService();
    logger = LogService.getLogger();

    await().until(() -> alertingService.hasAlertListener(managerMember, SEVERE));
  }

  private void changeAlertLevel(AlertLevel alertLevel) {
    managerVM.invoke(() -> {
      distributedSystemMXBean.changeAlertLevel(alertLevel.name());
    });

    memberVM.invoke(() -> {
      if (alertLevel == NONE) {
        await().until(() -> !alertingService.hasAlertListener(managerMember, alertLevel));
      } else {
        await().until(() -> alertingService.hasAlertListener(managerMember, alertLevel));
      }
    });
  }
}
