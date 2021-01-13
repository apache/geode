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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.management.internal.SystemManagementService.FEDERATING_MANAGER_FACTORY_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.JMXTest;

@Category(JMXTest.class)
public class FederatingManagerConcurrencyIntegrationTest {

  private InternalCache cache;
  private FederatingManager federatingManager;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    System.setProperty(FEDERATING_MANAGER_FACTORY_PROPERTY,
        FederatingManagerFactoryWithMockMessenger.class.getName());

    cache = (InternalCache) new CacheFactory()
        .set(LOCATORS, "")
        .create();

    SystemManagementService managementService =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    managementService.createManager();
    federatingManager = managementService.getFederatingManager();
    federatingManager.startManager();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testFederatingManagerConcurrency() throws UnknownHostException {
    InternalDistributedMember member = member();

    for (int i = 1; i <= 100; i++) {
      federatingManager.addMember(member);
    }

    await().until(() -> !cache.getAllRegions().isEmpty());

    assertThat(federatingManager.latestException()).isNull();
  }

  private InternalDistributedMember member() throws UnknownHostException {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getInetAddress()).thenReturn(getLocalHost());
    when(member.getId()).thenReturn("member-1");
    return member;
  }

  private static class FederatingManagerFactoryWithMockMessenger
      implements FederatingManagerFactory {

    public FederatingManagerFactoryWithMockMessenger() {
      // must be public for instantiation by reflection
    }

    @Override
    public FederatingManager create(ManagementResourceRepo repo, InternalDistributedSystem system,
        SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
        StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
        Supplier<ExecutorService> executorServiceSupplier) {
      return new FederatingManager(repo, system, service, cache, statisticsFactory,
          statisticsClock, proxyFactory, mock(MemberMessenger.class), executorServiceSupplier);
    }
  }
}
