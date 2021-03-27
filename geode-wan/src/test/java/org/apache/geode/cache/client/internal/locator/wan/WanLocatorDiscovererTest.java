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
package org.apache.geode.cache.client.internal.locator.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

public class WanLocatorDiscovererTest {
  private LocatorMembershipListener locatorMembershipListener;
  private DistributionConfigImpl config;
  private final ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo =
      new ConcurrentHashMap<>();
  private String locators;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    locatorMembershipListener = mock(LocatorMembershipListener.class);
    DistributionLocatorId loc1 = new DistributionLocatorId(40401, "127.0.0.1", null, "loc1");
    DistributionLocatorId loc2 = new DistributionLocatorId(40402, "127.0.0.2", null, "loc2");
    DistributionLocatorId loc3 = new DistributionLocatorId(40403, "127.0.0.3", null, "loc3");
    DistributionLocatorId loc4 = new DistributionLocatorId(40404, "127.0.0.4", null, "loc4");
    Set<DistributionLocatorId> locatorSet = new HashSet<>();
    locatorSet.add(loc1);
    locatorSet.add(loc2);
    locatorSet.add(loc3);
    locatorSet.add(loc4);
    allLocatorsInfo.put(1, locatorSet);
    Set<String> serverLocatorSet = new HashSet<>();
    serverLocatorSet.add(loc1.toString());
    serverLocatorSet.add(loc2.toString());
    serverLocatorSet.add(loc3.toString());
    serverLocatorSet.add(loc4.toString());
    allServerLocatorsInfo.put(1, serverLocatorSet);
    config = mock(DistributionConfigImpl.class);
    locators = loc1.marshal();
  }

  @Test
  public void test_discover() {
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    when(config.getStartLocator()).thenReturn(DistributionConfig.DEFAULT_START_LOCATOR);
    when(config.getName()).thenReturn("loc1");
    when(config.getBindAddress()).thenReturn("127.0.0.1");
    when(config.getDistributedSystemId()).thenReturn(1);
    when(config.getLocators()).thenReturn(locators);
    when(config.getRemoteLocators()).thenReturn("");

    WanLocatorDiscovererImpl test_wan = new WanLocatorDiscovererImpl();
    test_wan.discover(40401, config, locatorMembershipListener, null);

    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
    verify(locatorMembershipListener, times(1)).getAllLocatorsInfo();
  }

}
