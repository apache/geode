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
package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class FederatingManagerIntegrationTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Mock
  public InternalCache cache;
  @Mock
  public InternalCacheForClientAccess cacheForClientAccess;
  @Mock
  public MBeanProxyFactory proxyFactory;
  @Mock
  public MemberMessenger messenger;
  @Mock
  public ManagementResourceRepo repo;
  @Mock
  public SystemManagementService service;
  @Mock
  public StatisticsFactory statisticsFactory;
  @Mock
  public StatisticsClock statisticsClock;
  @Mock
  public InternalDistributedSystem system;
  @Mock
  public DistributionManager distributionManager;

  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    when(cache.getCacheForProcessingClientRequests())
        .thenReturn(cacheForClientAccess);
    when(system.getDistributionManager())
        .thenReturn(distributionManager);
  }

  @Test
  public void restartDoesNotThrowIfOtherMembersExist() {
    when(distributionManager.getOtherDistributionManagerIds())
        .thenReturn(Collections.singleton(mock(InternalDistributedMember.class)));

    FederatingManager federatingManager =
        new FederatingManager(repo, system, service, cache, statisticsFactory,
            statisticsClock, proxyFactory, messenger, Executors::newSingleThreadExecutor);

    federatingManager.startManager();
    federatingManager.stopManager();

    assertThatCode(federatingManager::startManager)
        .doesNotThrowAnyException();
  }
}
