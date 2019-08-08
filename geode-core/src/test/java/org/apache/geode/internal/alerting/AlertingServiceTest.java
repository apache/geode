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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link AlertingService}.
 */
@Category(AlertingTest.class)
public class AlertingServiceTest {

  private AlertingProviderRegistry registry;
  private AlertingProvider provider;
  private DistributedMember member;

  private AlertingService alertingService;

  @Before
  public void setUp() {
    registry = mock(AlertingProviderRegistry.class);
    provider = mock(AlertingProvider.class);
    member = mock(DistributedMember.class);

    when(registry.getAlertingProvider()).thenReturn(provider);
    when(provider.hasAlertListener(eq(member), eq(AlertLevel.WARNING))).thenReturn(true);
    when(provider.removeAlertListener(eq(member))).thenReturn(true);

    alertingService = new AlertingService(registry);
  }

  @Test
  public void hasAlertListenerDelegates() {
    assertThat(alertingService.hasAlertListener(member, AlertLevel.WARNING)).isTrue();

    verify(provider).hasAlertListener(eq(member), eq(AlertLevel.WARNING));
  }

  @Test
  public void addAlertListenerDelegates() {
    alertingService.addAlertListener(member, AlertLevel.WARNING);

    verify(provider).addAlertListener(eq(member), eq(AlertLevel.WARNING));
  }

  @Test
  public void removeAlertListenerDelegates() {
    assertThat(alertingService.removeAlertListener(member)).isTrue();

    verify(provider).removeAlertListener(eq(member));
  }
}
