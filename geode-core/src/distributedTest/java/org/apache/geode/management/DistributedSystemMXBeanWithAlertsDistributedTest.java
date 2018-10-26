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
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.alerting.AlertLevel.ERROR;
import static org.apache.geode.internal.alerting.AlertLevel.NONE;
import static org.apache.geode.internal.alerting.AlertLevel.SEVERE;
import static org.apache.geode.internal.alerting.AlertLevel.WARNING;
import static org.apache.geode.management.JMXNotificationType.SYSTEM_ALERT;
import static org.apache.geode.management.JMXNotificationUserData.ALERT_LEVEL;
import static org.apache.geode.management.ManagementService.getManagementService;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedSystemName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import javax.management.InstanceNotFoundException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.alerting.AlertingService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.AlertingTest;
import org.apache.geode.test.junit.categories.ManagementTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for {@link DistributedSystemMXBean} with alerts. Extracted from
 * {@link DistributedSystemMXBeanDistributedTest}.
 */
@Category({ManagementTest.class, AlertingTest.class})
@SuppressWarnings("serial")
public class DistributedSystemMXBeanWithAlertsDistributedTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String MANAGER_NAME = "managerVM";
  private static final String MEMBER_NAME = "memberVM-";

  private static final NotificationFilter SYSTEM_ALERT_FILTER =
      notification -> notification.getType().equals(SYSTEM_ALERT);

  private static InternalCache cache;
  private static AlertingService alertingService;
  private static AlertNotificationListener alertNotificationListener;
  private static DistributedSystemMXBean distributedSystemMXBean;

  private DistributedMember managerMember;

  private String warningLevelMessage;
  private String errorLevelMessage;
  private String severeLevelMessage;

  private VM managerVM;
  private VM memberVM1;
  private VM memberVM2;
  private VM memberVM3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    warningLevelMessage = WARNING.name() + " level alert in " + testName.getMethodName();
    errorLevelMessage = ERROR.name() + " level alert in " + testName.getMethodName();
    severeLevelMessage = SEVERE.name() + " level alert in " + testName.getMethodName();

    managerVM = getVM(0);
    memberVM1 = getVM(1);
    memberVM2 = getVM(2);
    memberVM3 = getVM(3);

    managerMember = managerVM.invoke(() -> createManager());

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        createMember(memberVM);
      });
    }
  }

  @After
  public void tearDown() throws Exception {
    for (VM vm : toArray(managerVM, memberVM1, memberVM2, memberVM3)) {
      vm.invoke(() -> {
        if (cache != null) {
          cache.close();
        }
        cache = null;
        alertingService = null;
        alertNotificationListener = null;
        distributedSystemMXBean = null;
      });
    }
  }

  @Test
  public void managerAddsAlertListenerToEachMember() {
    // default AlertLevel is SEVERE

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        await().untilAsserted(
            () -> assertThat(alertingService.hasAlertListener(managerMember, SEVERE)).isTrue());
      });
    }
  }

  @Test
  public void managerReceivesRemoteAlertAtAlertLevel() {
    memberVM1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
        logger.fatal(severeLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(1));
    });
  }

  @Test
  public void managerDoesNotReceiveRemoteAlertBelowAlertLevel() {
    memberVM1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(warningLevelMessage)) {
        logger.warn(warningLevelMessage);
      }
      try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
        logger.error(errorLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getWarningAlertCount()).isEqualTo(0);
      assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(0);
    });
  }

  @Test
  public void managerReceivesRemoteAlertAboveAlertLevel() {
    changeAlertLevel(WARNING);

    memberVM1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
        logger.error(errorLevelMessage);
      }
      try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
        logger.fatal(severeLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(1);
      assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(1);
    });
  }

  @Test
  public void managerReceivesLocalAlertAtAlertLevel() {
    managerVM.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
        logger.fatal(severeLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(1));
    });
  }

  @Test
  public void managerDoesNotReceiveLocalAlertBelowAlertLevel() {
    managerVM.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(warningLevelMessage)) {
        logger.warn(warningLevelMessage);
      }
      try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
        logger.error(errorLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getWarningAlertCount()).isEqualTo(0);
      assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(0);
    });
  }

  /**
   * Fails due to GEODE-5923: JMX manager only receives local Alerts at the default AlertLevel
   *
   * <p>
   * The JMX manager's local AlertListener for itself remains stuck at {@code SEVERE} even after
   * invoking {@link DistributedSystemMXBean#changeAlertLevel(String)}.
   */
  @Test
  @Ignore("GEODE-5923")
  public void managerReceivesLocalAlertAboveAlertLevel() {
    changeAlertLevel(WARNING);

    managerVM.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
        logger.error(errorLevelMessage);
      }
      try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
        logger.fatal(severeLevelMessage);
      }
    });

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(1);
      assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(1);
    });
  }

  @Test
  public void managerReceivesAlertsFromAllMembers() {
    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
          logger.fatal(severeLevelMessage);
        }
      });
    }

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(3));
    });
  }

  @Test
  public void managerReceivesAlertsFromAllMembersAtAlertLevelAndAbove() {
    changeAlertLevel(WARNING);

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        try (IgnoredException ie = addIgnoredException(warningLevelMessage)) {
          logger.warn(warningLevelMessage);
        }
        try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
          logger.error(errorLevelMessage);
        }
        try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
          logger.fatal(severeLevelMessage);
        }
      });
    }

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getWarningAlertCount()).isEqualTo(3));
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(3));
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(3));
    });
  }

  @Test
  public void managerDoesNotReceiveAlertsAtAlertLevelNone() {
    changeAlertLevel(NONE);

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        try (IgnoredException ie = addIgnoredException(warningLevelMessage)) {
          logger.warn(warningLevelMessage);
        }
        try (IgnoredException ie = addIgnoredException(errorLevelMessage)) {
          logger.error(errorLevelMessage);
        }
        try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
          logger.fatal(severeLevelMessage);
        }
      });
    }

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getWarningAlertCount()).isEqualTo(0);
      assertThat(alertNotificationListener.getErrorAlertCount()).isEqualTo(0);
      assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(0);
    });
  }

  @Test
  public void managerMissesAnyAlertsBeforeItStarts() {
    // close managerVM so we can recreate it AFTER generating alerts in memberVMs
    managerVM.invoke(() -> cache.close());

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
          logger.fatal(severeLevelMessage);
        }
      });
    }

    managerVM.invoke(() -> createManager());

    managerMember = managerVM.invoke(() -> cache.getDistributionManager().getId());

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        await().until(() -> alertingService.hasAlertListener(managerMember, SEVERE));
      });
    }

    // managerVM should have missed the alerts from BEFORE it started

    managerVM.invoke(() -> {
      assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(0);
    });

    // managerVM should now receive any new alerts though

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        try (IgnoredException ie = addIgnoredException(severeLevelMessage)) {
          logger.fatal(severeLevelMessage);
        }
      });
    }

    managerVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(alertNotificationListener.getSevereAlertCount()).isEqualTo(3));
    });
  }

  private DistributedMember createManager() throws InstanceNotFoundException {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, MANAGER_NAME);
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");
    config.setProperty(HTTP_SERVICE_PORT, "0");

    cache = (InternalCache) new CacheFactory(config).create();
    alertingService = cache.getInternalDistributedSystem().getAlertingService();

    alertNotificationListener = new AlertNotificationListener();
    getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(),
        alertNotificationListener, SYSTEM_ALERT_FILTER, null);

    distributedSystemMXBean = getManagementService(cache).getDistributedSystemMXBean();

    return cache.getDistributionManager().getId();
  }

  private void createMember(VM memberVM) {
    Properties config = getDistributedSystemProperties();
    config.setProperty(NAME, MEMBER_NAME + memberVM.getId());
    config.setProperty(JMX_MANAGER, "false");

    cache = (InternalCache) new CacheFactory(config).create();
    alertingService = cache.getInternalDistributedSystem().getAlertingService();

    await().until(() -> alertingService.hasAlertListener(managerMember, SEVERE));
  }

  private void changeAlertLevel(AlertLevel alertLevel) {
    managerVM.invoke(() -> {
      distributedSystemMXBean.changeAlertLevel(alertLevel.name());
    });

    for (VM memberVM : toArray(memberVM1, memberVM2, memberVM3)) {
      memberVM.invoke(() -> {
        if (alertLevel == NONE) {
          await().until(() -> !alertingService.hasAlertListener(managerMember, alertLevel));
        } else {
          await().until(() -> alertingService.hasAlertListener(managerMember, alertLevel));
        }
      });
    }
  }

  // TODO:KIRK: convert to mockito spy
  private class AlertNotificationListener implements NotificationListener {

    private int warningAlertCount;
    private int errorAlertCount;
    private int severeAlertCount;

    @Override
    public synchronized void handleNotification(final Notification notification,
        final Object handback) {
      errorCollector.checkThat(notification, notNullValue());

      Map<String, String> notificationUserData = (Map<String, String>) notification.getUserData();

      if (notificationUserData.get(ALERT_LEVEL).equalsIgnoreCase(WARNING.name())) {
        errorCollector.checkThat(notification.getMessage(), Matchers.equalTo(warningLevelMessage));
        warningAlertCount++;
      }
      if (notificationUserData.get(ALERT_LEVEL).equalsIgnoreCase(ERROR.name())) {
        errorCollector.checkThat(notification.getMessage(), Matchers.equalTo(errorLevelMessage));
        errorAlertCount++;
      }
      if (notificationUserData.get(ALERT_LEVEL).equalsIgnoreCase(SEVERE.name())) {
        errorCollector.checkThat(notification.getMessage(), Matchers.equalTo(severeLevelMessage));
        severeAlertCount++;
      }
    }

    synchronized int getWarningAlertCount() {
      return warningAlertCount;
    }

    synchronized int getErrorAlertCount() {
      return errorAlertCount;
    }

    synchronized int getSevereAlertCount() {
      return severeAlertCount;
    }
  }
}
