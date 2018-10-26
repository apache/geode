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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.alerting.AlertingProvider;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link PausableAlertAppender}.
 */
@Category(LoggingTest.class)
public class PausableAlertAppenderTest {

  private DistributedMember member;

  private PausableAlertAppender pausableAlertAppender;
  private AlertingProvider asAlertingProvider;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    member = mock(DistributedMember.class);

    pausableAlertAppender = new PausableAlertAppender(testName.getMethodName(), null, null);
    asAlertingProvider = pausableAlertAppender;
  }

  @Test
  public void alertListenersIsEmptyByDefault() {
    assertThat(pausableAlertAppender.getAlertListeners()).isEmpty();
  }

  @Test
  public void hasAlertListenerReturnsFalseByDefault() {
    asAlertingProvider.hasAlertListener(member, AlertLevel.WARNING);
  }

  @Test
  public void addAlertListenerAddsListener() {
    asAlertingProvider.addAlertListener(member, AlertLevel.WARNING);
    assertThat(pausableAlertAppender.getAlertListeners())
        .contains(new AlertListener(Level.WARN, member));
  }

  @Test
  public void hasAlertListenerReturnsTrueIfListenerExists() {
    asAlertingProvider.addAlertListener(member, AlertLevel.WARNING);
    assertThat(asAlertingProvider.hasAlertListener(member, AlertLevel.WARNING)).isTrue();
  }

  @Test
  public void removeAlertListenerDoesNothingByDefault() {
    asAlertingProvider.removeAlertListener(member);
    assertThat(pausableAlertAppender.getAlertListeners()).isEmpty();
  }

  @Test
  public void removeAlertListenerDoesNothingIfMemberDoesNotMatch() {
    asAlertingProvider.addAlertListener(member, AlertLevel.WARNING);

    asAlertingProvider.removeAlertListener(mock(DistributedMember.class));

    assertThat(asAlertingProvider.hasAlertListener(member, AlertLevel.WARNING)).isTrue();
  }

  @Test
  public void removeAlertListenerRemovesListener() {
    asAlertingProvider.addAlertListener(member, AlertLevel.WARNING);

    asAlertingProvider.removeAlertListener(member);

    assertThat(asAlertingProvider.hasAlertListener(member, AlertLevel.WARNING)).isFalse();
  }

  @Test
  public void addAlertListenerWithAlertLevelNoneDoesNothing() {
    asAlertingProvider.addAlertListener(member, AlertLevel.NONE);
    assertThat(pausableAlertAppender.getAlertListeners()).isEmpty();
  }

  @Test
  public void hasAlertListenerReturnsFalseIfAlertLevelIsNone() {
    asAlertingProvider.addAlertListener(member, AlertLevel.WARNING);
    assertThat(asAlertingProvider.hasAlertListener(member, AlertLevel.NONE)).isFalse();
  }

  @Test
  public void addAlertListenerOrdersByAscendingAlertLevel() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    asAlertingProvider.addAlertListener(member3, AlertLevel.WARNING);
    asAlertingProvider.addAlertListener(member1, AlertLevel.SEVERE);
    asAlertingProvider.addAlertListener(member2, AlertLevel.ERROR);

    AlertListener listener1 = new AlertListener(Level.WARN, member3);
    AlertListener listener2 = new AlertListener(Level.ERROR, member2);
    AlertListener listener3 = new AlertListener(Level.FATAL, member1);

    assertThat(pausableAlertAppender.getAlertListeners()).containsExactly(listener1, listener2,
        listener3);
  }

  @Test
  public void removeAlertListenerMaintainsExistingOrder() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    asAlertingProvider.addAlertListener(member3, AlertLevel.WARNING);
    asAlertingProvider.addAlertListener(member1, AlertLevel.SEVERE);
    asAlertingProvider.addAlertListener(member2, AlertLevel.ERROR);

    AlertListener listener1 = new AlertListener(Level.WARN, member3);
    AlertListener listener3 = new AlertListener(Level.FATAL, member1);

    assertThat(pausableAlertAppender.removeAlertListener(member2)).isTrue();

    assertThat(pausableAlertAppender.getAlertListeners()).containsExactly(listener1, listener3);
  }

  @Test
  public void addAlertListenerOrdersByDescendingAddIfAlertLevelMatches() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    DistributedMember member3 = mock(DistributedMember.class);

    asAlertingProvider.addAlertListener(member3, AlertLevel.WARNING);
    asAlertingProvider.addAlertListener(member1, AlertLevel.WARNING);
    asAlertingProvider.addAlertListener(member2, AlertLevel.WARNING);

    AlertListener listener1 = new AlertListener(Level.WARN, member2);
    AlertListener listener2 = new AlertListener(Level.WARN, member1);
    AlertListener listener3 = new AlertListener(Level.WARN, member3);

    assertThat(pausableAlertAppender.getAlertListeners()).containsExactly(listener1, listener2,
        listener3);
  }
}
