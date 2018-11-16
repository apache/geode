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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.alerting.AlertLevel.ERROR;
import static org.apache.geode.internal.alerting.AlertLevel.NONE;
import static org.apache.geode.internal.alerting.AlertLevel.SEVERE;
import static org.apache.geode.internal.alerting.AlertLevel.WARNING;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Integration tests for adding and removing {@code Alert} listeners.
 */
@Category(AlertingTest.class)
public class AlertListenerRegistrationIntegrationTest {

  private InternalDistributedSystem system;
  private DistributedMember member;
  private AlertingService alertingService;

  @Before
  public void setUp() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);
    member = system.getDistributedMember();
    alertingService = system.getAlertingService();
  }

  @After
  public void tearDown() {
    system.disconnect();
  }

  @Test
  public void alertingProviderIsAlertAppender() {
    assertThat(alertingService.getAlertingProviderRegistry().getAlertingProvider())
        .isInstanceOf(AlertAppender.class);
  }

  @Test
  public void hasAlertListenerIsFalseByDefault() {
    for (AlertLevel alertLevel : AlertLevel.values()) {
      assertThat(alertingService.hasAlertListener(member, alertLevel)).isFalse();
    }
  }

  @Test
  public void hasAlertListenerIsTrueAfterAdding() {
    alertingService.addAlertListener(member, WARNING);

    assertThat(alertingService.hasAlertListener(member, WARNING)).isTrue();
  }

  @Test
  public void hasAlertListenerIsTrueOnlyForLevelWarning() {
    alertingService.addAlertListener(member, WARNING);

    for (AlertLevel alertLevel : AlertLevel.values()) {
      if (alertLevel != WARNING) {
        assertThat(alertingService.hasAlertListener(member, alertLevel)).isFalse();
      }
    }
  }

  @Test
  public void hasAlertListenerIsTrueOnlyForLevelError() {
    alertingService.addAlertListener(member, ERROR);

    for (AlertLevel alertLevel : AlertLevel.values()) {
      if (alertLevel != ERROR) {
        assertThat(alertingService.hasAlertListener(member, alertLevel)).isFalse();
      }
    }
  }

  @Test
  public void hasAlertListenerIsTrueOnlyForLevelSevere() {
    alertingService.addAlertListener(member, SEVERE);

    for (AlertLevel alertLevel : AlertLevel.values()) {
      if (alertLevel != SEVERE) {
        assertThat(alertingService.hasAlertListener(member, alertLevel)).isFalse();
      }
    }
  }

  @Test
  public void addAlertListenerDoesNothingForLevelNone() {
    alertingService.addAlertListener(member, NONE);

    for (AlertLevel alertLevel : AlertLevel.values()) {
      assertThat(alertingService.hasAlertListener(member, alertLevel)).isFalse();
    }
  }

  @Test
  public void removeAlertListenerReturnsFalseByDefault() {
    assertThat(alertingService.removeAlertListener(member)).isFalse();
  }

  @Test
  public void removeAlertListenerReturnsFalseAfterAddingForLevelNone() {
    alertingService.addAlertListener(member, NONE);

    assertThat(alertingService.removeAlertListener(member)).isFalse();
  }

  @Test
  public void removeAlertListenerReturnsTrueAfterAdding() {
    alertingService.addAlertListener(member, WARNING);

    assertThat(alertingService.removeAlertListener(member)).isTrue();
  }

  @Test
  public void hasAlertListenerIsFalseAfterRemoving() {
    alertingService.addAlertListener(member, WARNING);
    alertingService.removeAlertListener(member);

    assertThat(alertingService.hasAlertListener(member, WARNING)).isFalse();
  }

  @Test
  public void systemHasAlertListenerForMemberIsFalseByDefault() {
    assertThat(system.hasAlertListenerFor(member)).isFalse();
    for (AlertLevel alertLevel : AlertLevel.values()) {
      assertThat(system.hasAlertListenerFor(member, alertLevel.intLevel())).isFalse();
    }
  }

  @Test
  public void systemHasAlertListenerForAlertLevelIsFalseByDefault() {
    for (AlertLevel alertLevel : AlertLevel.values()) {
      assertThat(system.hasAlertListenerFor(member, alertLevel.intLevel())).isFalse();
    }
  }

  @Test
  public void systemHasAlertListenerIsTrueAfterAdding() {
    alertingService.addAlertListener(member, WARNING);

    assertThat(system.hasAlertListenerFor(member, WARNING.intLevel())).isTrue();
  }

  @Test
  public void systemHasAlertListenerIsFalseForOtherLevels() {
    alertingService.addAlertListener(member, WARNING);

    for (AlertLevel alertLevel : AlertLevel.values()) {
      if (alertLevel != WARNING) {
        assertThat(system.hasAlertListenerFor(member, alertLevel.intLevel())).isFalse();
      }
    }
  }
}
