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

package org.apache.geode.cache.wan.internal.client.locator;

import static java.util.Collections.singleton;
import static org.apache.geode.cache.wan.internal.client.locator.LocatorHelper.addServerLocator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.jupiter.api.Test;

import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.inet.LocalHostUtil;

class LocatorHelperTest {
  private final LocatorMembershipListener locatorMembershipListener =
      mock(LocatorMembershipListener.class);

  private final ConcurrentMap<Integer, Set<String>> locators = new ConcurrentHashMap<>();

  LocatorHelperTest() {
    when(locatorMembershipListener.getAllServerLocatorsInfo()).thenReturn(locators);
  }

  @Test
  void addServerLocatorDoesNotAddSameLocatorWithSameDistributedSystemId() {
    final DistributionLocatorId locator = new DistributionLocatorId("localhost[1234]");

    addServerLocator(1, locatorMembershipListener, locator);
    assertThat(locators).containsExactly(entry(1, singleton("localhost[1234]")));

    addServerLocator(1, locatorMembershipListener, locator);
    assertThat(locators).containsExactly(entry(1, singleton("localhost[1234]")));
  }

  @Test
  void addServerLocatorDoesNotAddEqualLocatorWithSameDistributedSystemId() {
    final DistributionLocatorId locator1 = new DistributionLocatorId("localhost[1234]");
    final DistributionLocatorId locator2 = new DistributionLocatorId("localhost[1234]");

    addServerLocator(1, locatorMembershipListener, locator1);
    assertThat(locators).containsExactly(entry(1, singleton("localhost[1234]")));

    addServerLocator(1, locatorMembershipListener, locator2);
    assertThat(locators).containsExactly(entry(1, singleton("localhost[1234]")));
  }

  @Test
  void addServerLocatorAddsUniqueLocatorWithSameDistributedSystemId() {
    final DistributionLocatorId locator1 = new DistributionLocatorId("localhost[1234]");
    final DistributionLocatorId locator2 = new DistributionLocatorId("localhost[1235]");

    addServerLocator(1, locatorMembershipListener, locator1);
    addServerLocator(1, locatorMembershipListener, locator2);

    assertThat(locators).containsOnlyKeys(1);
    assertThat(locators.get(1)).containsExactlyInAnyOrder("localhost[1234]", "localhost[1235]");
  }

  @Test
  void addServerLocatorDoesNotAddsLocatorWithDifferentDistributedSystemId() {
    final DistributionLocatorId locator1 = new DistributionLocatorId("localhost[1234]");
    final DistributionLocatorId locator2 = new DistributionLocatorId("localhost[1235]");

    addServerLocator(1, locatorMembershipListener, locator1);
    addServerLocator(2, locatorMembershipListener, locator2);

    assertThat(locators).containsOnly(
        entry(1, singleton("localhost[1234]")),
        entry(2, singleton("localhost[1235]")));
  }

  @Test
  void addServerLocatorUsesHostnameForClients() {
    final DistributionLocatorId locator =
        new DistributionLocatorId(1234, "bind-address.example.com",
            "hostname-for-clients.example.com");

    addServerLocator(1, locatorMembershipListener, locator);
    assertThat(locators)
        .containsExactly(entry(1, singleton("hostname-for-clients.example.com[1234]")));
  }

  @Test
  void addServerLocatorUsesBindAddress() {
    final DistributionLocatorId locator =
        new DistributionLocatorId(1234, "bind-address.example.com");

    addServerLocator(1, locatorMembershipListener, locator);
    assertThat(locators).containsExactly(entry(1, singleton("bind-address.example.com[1234]")));
  }

  @Test
  void addServerLocatorUsesLocalHostNameLocatorWhenBindAddressAndHostnameForClientNotSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final Locator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    addServerLocator(1, locatorMembershipListener, locatorId);
    assertThat(locators)
        .containsExactly(entry(1, singleton(localHost.getCanonicalHostName() + "[1234]")));
  }

  @Test
  void addServerLocatorUsesBindAddressWhenLocatorBindAddressSetAndHostnameForClientNotSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final InternalLocator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);
    when(locator.getBindAddressString()).thenReturn("bind-address.example.com");

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    addServerLocator(1, locatorMembershipListener, locatorId);
    assertThat(locators).containsExactly(entry(1, singleton("bind-address.example.com[1234]")));
  }

  @Test
  void addServerLocatorUsesBindAddressWhenLocatorBindAddressAndHostnameForClientSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final InternalLocator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);
    when(locator.getBindAddressString()).thenReturn("bind-address.example.com");
    when(locator.getHostnameForClients()).thenReturn("hostname-for-clients.example.com");

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    addServerLocator(1, locatorMembershipListener, locatorId);
    assertThat(locators)
        .containsExactly(entry(1, singleton("hostname-for-clients.example.com[1234]")));
  }
}
