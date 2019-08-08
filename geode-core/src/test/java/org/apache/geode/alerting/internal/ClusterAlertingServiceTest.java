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
package org.apache.geode.alerting.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link ClusterAlertingService}.
 */
@Category(AlertingTest.class)
public class ClusterAlertingServiceTest {

  private DistributedMember member;
  private ClusterAlertingService alertingService;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    member = mock(DistributedMember.class);
    alertingService = new ClusterAlertingService();
  }

  @Test
  public void alertListenersIsEmptyByDefault() {
    assertThat(alertingService.getAlertListeners()).isEmpty();
  }

  @Test
  public void hasAlertListenerReturnsFalseByDefault() {
    alertingService.hasAlertListener(member, AlertLevel.WARNING);
  }

  @Test
  public void addAlertListenerAddsListener() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);
    assertThat(alertingService.getAlertListeners())
        .contains(new AlertListener(AlertLevel.WARNING, member));
  }

  @Test
  public void hasAlertListenerReturnsTrueIfListenerExists() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);
    assertThat(alertingService.hasAlertListener(member, AlertLevel.WARNING)).isTrue();
  }

  @Test
  public void removeAlertListenerDoesNothingByDefault() {
    alertingService.removeAlertListener(member);
    assertThat(alertingService.getAlertListeners()).isEmpty();
  }

  @Test
  public void removeAlertListenerDoesNothingIfMemberDoesNotMatch() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    alertingService.removeAlertListener(mock(DistributedMember.class));

    assertThat(alertingService.hasAlertListener(member, AlertLevel.WARNING)).isTrue();
  }

  @Test
  public void removeAlertListenerRemovesListener() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    alertingService.removeAlertListener(member);

    assertThat(alertingService.hasAlertListener(member, AlertLevel.WARNING)).isFalse();
  }

  @Test
  public void addAlertListenerWithAlertLevelNoneDoesNothing() {
    alertingService.addAlertListener(member, AlertLevel.NONE);
    assertThat(alertingService.getAlertListeners()).isEmpty();
  }

  @Test
  public void hasAlertListenerReturnsFalseIfAlertLevelIsNone() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);
    assertThat(alertingService.hasAlertListener(member, AlertLevel.NONE)).isFalse();
  }

  @Test
  public void addAlertListenerOrdersByAscendingAlertLevel() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    alertingService.addAlertListener(member3, AlertLevel.WARNING);
    alertingService.addAlertListener(member1, AlertLevel.SEVERE);
    alertingService.addAlertListener(member2, AlertLevel.ERROR);

    AlertListener listener1 = new AlertListener(AlertLevel.WARNING, member3);
    AlertListener listener2 = new AlertListener(AlertLevel.ERROR, member2);
    AlertListener listener3 = new AlertListener(AlertLevel.SEVERE, member1);

    assertThat(alertingService.getAlertListeners()).containsExactly(listener1, listener2,
        listener3);
  }

  @Test
  public void removeAlertListenerMaintainsExistingOrder() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    alertingService.addAlertListener(member3, AlertLevel.WARNING);
    alertingService.addAlertListener(member1, AlertLevel.SEVERE);
    alertingService.addAlertListener(member2, AlertLevel.ERROR);

    AlertListener listener1 = new AlertListener(AlertLevel.WARNING, member3);
    AlertListener listener3 = new AlertListener(AlertLevel.SEVERE, member1);

    assertThat(alertingService.removeAlertListener(member2)).isTrue();

    assertThat(alertingService.getAlertListeners()).containsExactly(listener1, listener3);
  }

  @Test
  public void addAlertListenerOrdersByDescendingAddIfAlertLevelMatches() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    alertingService.addAlertListener(member3, AlertLevel.WARNING);
    alertingService.addAlertListener(member1, AlertLevel.WARNING);
    alertingService.addAlertListener(member2, AlertLevel.WARNING);

    AlertListener listener1 = new AlertListener(AlertLevel.WARNING, member2);
    AlertListener listener2 = new AlertListener(AlertLevel.WARNING, member1);
    AlertListener listener3 = new AlertListener(AlertLevel.WARNING, member3);

    assertThat(alertingService.getAlertListeners()).containsExactly(listener1, listener2,
        listener3);
  }
}
