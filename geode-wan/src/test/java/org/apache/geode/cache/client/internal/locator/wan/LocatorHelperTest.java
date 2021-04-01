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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.wan.internal.client.locator.LocatorHelper;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

public class LocatorHelperTest {
  private LocatorMembershipListener locatorMembershipListener;
  private final ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo =
      new ConcurrentHashMap<>();

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
  }

  @Test
  public void testAddLocator_addToList() {
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);
    when(locatorMembershipListener.getAllServerLocatorsInfo()).thenReturn(allServerLocatorsInfo);

    DistributionLocatorId locator = new DistributionLocatorId(40405, "127.0.0.5", null, "loc5");

    assertThat(LocatorHelper.addLocator(1, locator, locatorMembershipListener, null)).isTrue();
    assertThat(allLocatorsInfo.get(1)).hasSize(5);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(5);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, locator, null);
  }

  @Test
  public void testAddLocator_replaceInList() {
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);
    when(locatorMembershipListener.getAllServerLocatorsInfo()).thenReturn(allServerLocatorsInfo);

    DistributionLocatorId locator = new DistributionLocatorId(40405, "127.0.0.5", null, "loc4");

    assertThat(LocatorHelper.addLocator(1, locator, locatorMembershipListener, null)).isTrue();
    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, locator, null);
  }

  @Test
  public void testAddLocator_noUpdate() {
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    DistributionLocatorId locator = new DistributionLocatorId(40404, "127.0.0.4", null, "loc4");

    assertThat(LocatorHelper.addLocator(1, locator, locatorMembershipListener, null)).isFalse();
    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
    verify(locatorMembershipListener, times(0)).locatorJoined(1, locator, null);
  }

  @Test
  public void testAddLocator_modifyMemberName() {
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    DistributionLocatorId locator = new DistributionLocatorId(40404, "127.0.0.4", null, "loc5");

    assertThat(LocatorHelper.addLocator(1, locator, locatorMembershipListener, null)).isTrue();
    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, locator, null);
  }

  @Test
  public void testAddExchangedLocators_noUpdate() {
    Map<Integer, Set<DistributionLocatorId>> responseLocatorsInfo =
        new HashMap<>();

    DistributionLocatorId loc1 = new DistributionLocatorId(40401, "127.0.0.1", null, "loc1");
    DistributionLocatorId loc2 = new DistributionLocatorId(40402, "127.0.0.2", null, "loc2");
    DistributionLocatorId loc3 = new DistributionLocatorId(40403, "127.0.0.3", null, "loc3");
    DistributionLocatorId loc4 = new DistributionLocatorId(40404, "127.0.0.4", null, "loc4");
    Set<DistributionLocatorId> responseSet = new HashSet<>();
    responseSet.add(loc1);
    responseSet.add(loc2);
    responseSet.add(loc3);
    responseSet.add(loc4);
    responseLocatorsInfo.put(1, responseSet);
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(allLocatorsInfo);

    assertThat(
        LocatorHelper.addExchangedLocators(responseLocatorsInfo, locatorMembershipListener))
            .isFalse();
    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
  }

  @Test
  public void testAddExchangedLocators_initialUpdate() {
    ConcurrentMap<Integer, Set<DistributionLocatorId>> emptyLocatorList =
        new ConcurrentHashMap<>();
    ConcurrentMap<Integer, Set<String>> emptyServerLocatorList =
        new ConcurrentHashMap<>();

    Map<Integer, Set<DistributionLocatorId>> responseLocatorsInfo =
        new HashMap<>();

    DistributionLocatorId loc1 = new DistributionLocatorId(40401, "127.0.0.1", null, "loc1");
    DistributionLocatorId loc2 = new DistributionLocatorId(40402, "127.0.0.2", null, "loc2");
    DistributionLocatorId loc3 = new DistributionLocatorId(40403, "127.0.0.3", null, "loc3");
    DistributionLocatorId loc4 = new DistributionLocatorId(40404, "127.0.0.4", null, "loc4");
    Set<DistributionLocatorId> responseSet = new HashSet<>();
    responseSet.add(loc1);
    responseSet.add(loc2);
    responseSet.add(loc3);
    responseSet.add(loc4);
    responseLocatorsInfo.put(1, responseSet);
    when(locatorMembershipListener.getAllLocatorsInfo()).thenReturn(emptyLocatorList);
    when(locatorMembershipListener.getAllServerLocatorsInfo()).thenReturn(emptyServerLocatorList);

    assertThat(
        LocatorHelper.addExchangedLocators(responseLocatorsInfo, locatorMembershipListener))
            .isTrue();
    assertThat(allLocatorsInfo.get(1)).hasSize(4);
    assertThat(allServerLocatorsInfo.get(1)).hasSize(4);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, loc1, null);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, loc2, null);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, loc3, null);
    verify(locatorMembershipListener, times(1)).locatorJoined(1, loc4, null);
  }

}
