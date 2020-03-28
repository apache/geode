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
import static org.apache.geode.alerting.internal.spi.AlertLevel.ERROR;
import static org.apache.geode.alerting.internal.spi.AlertLevel.NONE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.SEVERE;
import static org.apache.geode.alerting.internal.spi.AlertLevel.WARNING;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.management.JMXNotificationType.SYSTEM_ALERT;
import static org.apache.geode.management.JMXNotificationUserData.ALERT_LEVEL;
import static org.apache.geode.management.ManagementService.getManagementService;
import static org.apache.geode.management.internal.MBeanJMXAdapter.getDistributedSystemName;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.internal.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.management.InstanceNotFoundException;
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
import org.mockito.ArgumentCaptor;

import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
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
  private static final long TIMEOUT = getTimeout().toMillis();
  private static final NotificationFilter SYSTEM_ALERT_FILTER =
      notification -> notification.getType().equals(SYSTEM_ALERT);

  private static InternalCache cache;
  private static AlertingService alertingService;
  private static NotificationListener notificationListener;
  private static DistributedSystemMXBean distributedSystemMXBean;

  private DistributedMember managerMember;

  private String warningLevelMessage;
  private String errorLevelMessage;
  private String severeLevelMessage;

  private Alert warningAlert;
  private Alert errorAlert;
  private Alert severeAlert;

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

    warningAlert = new Alert(WARNING, warningLevelMessage);
    errorAlert = new Alert(ERROR, errorLevelMessage);
    severeAlert = new Alert(SEVERE, severeLevelMessage);

    managerVM = getVM(0);
    memberVM1 = getVM(1);
    memberVM2 = getVM(2);
    memberVM3 = getVM(3);

    managerMember = managerVM.invoke(() -> createManager());
    IgnoredException.addIgnoredException("Cannot form connection to alert listener");

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
        notificationListener = null;
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
      assertThat(captureAlert()).isEqualTo(severeAlert);
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
      verifyZeroInteractions(notificationListener);
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
      assertThat(captureAllAlerts(2)).contains(errorAlert, severeAlert);
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
      assertThat(captureAlert()).isEqualTo(severeAlert);
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
      verifyZeroInteractions(notificationListener);
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
      assertThat(captureAllAlerts(2)).contains(errorAlert, severeAlert);
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
      assertThat(captureAllAlerts(3)).contains(severeAlert, severeAlert, severeAlert);
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
      assertThat(captureAllAlerts(9)).contains(warningAlert, warningAlert, warningAlert, errorAlert,
          errorAlert, errorAlert, severeAlert, severeAlert, severeAlert);
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
      verifyZeroInteractions(notificationListener);
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
      verifyZeroInteractions(notificationListener);
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
      assertThat(captureAllAlerts(3)).contains(severeAlert, severeAlert, severeAlert);
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

    notificationListener = spy(NotificationListener.class);
    getPlatformMBeanServer().addNotificationListener(getDistributedSystemName(),
        notificationListener, SYSTEM_ALERT_FILTER, null);

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

  private Notification captureNotification() {
    ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
    verify(notificationListener, timeout(TIMEOUT)).handleNotification(captor.capture(), isNull());
    return captor.getValue();
  }

  private List<Notification> captureAllNotifications(int count) {
    ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
    verify(notificationListener, timeout(TIMEOUT).times(count)).handleNotification(captor.capture(),
        isNull());
    return captor.getAllValues();
  }

  private Alert captureAlert() {
    Notification notification = captureNotification();
    return new Alert(getAlertLevel(notification), notification.getMessage());
  }

  private List<Alert> captureAllAlerts(int count) {
    List<Alert> alerts = new ArrayList<>();
    for (Notification notification : captureAllNotifications(count)) {
      alerts.add(new Alert(getAlertLevel(notification), notification.getMessage()));
    }
    return alerts;
  }

  private static AlertLevel getAlertLevel(Notification notification) {
    return AlertLevel.valueOf(getUserData(notification).get(ALERT_LEVEL).toUpperCase());
  }

  private static Map<String, String> getUserData(Notification notification) {
    return (Map<String, String>) notification.getUserData();
  }

  /**
   * Simple struct with {@link AlertLevel} and {@code String} message with {@code equals}
   * implemented to compare both fields.
   */
  private static class Alert implements Serializable {

    private final AlertLevel level;
    private final String message;

    Alert(AlertLevel level, String message) {
      this.level = level;
      this.message = message;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      Alert alert = (Alert) obj;
      return level == alert.level && Objects.equals(message, alert.message);
    }

    @Override
    public int hashCode() {
      return Objects.hash(level, message);
    }
  }
}
