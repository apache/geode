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

package org.apache.geode.internal.admin.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.jupiter.api.Test;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.inet.LocalHostUtil;

class DistributionLocatorIdTest {

  @Test
  void testEquals() throws UnknownHostException {
    InetAddress address = InetAddress.getLocalHost();
    DistributionLocatorId distributionLocatorId1 =
        new DistributionLocatorId(address, 40404, "127.0.0.1", null);
    DistributionLocatorId distributionLocatorId2 =
        new DistributionLocatorId(address, 40404, "127.0.0.1", null);
    DistributionLocatorId distributionLocatorId3 =
        new DistributionLocatorId(InetAddress.getByName("localhost"),
            50505, "128.0.0.1", null);

    assertThat(distributionLocatorId1).isEqualTo(distributionLocatorId1);
    assertThat(distributionLocatorId1).isEqualTo(distributionLocatorId2);
    assertThat(distributionLocatorId1.equals(distributionLocatorId3)).isFalse();
  }

  @Test
  void testEquals_and_DetailCompare() {
    DistributionLocatorId distributionLocatorId1 =
        new DistributionLocatorId(40404, "127.0.0.1", null);
    DistributionLocatorId distributionLocatorId2 =
        new DistributionLocatorId(40404, "127.0.0.1", "127.0.1.0", "member2");
    DistributionLocatorId distributionLocatorId3 =
        new DistributionLocatorId(40404, "127.0.0.1", null, "member3");
    DistributionLocatorId distributionLocatorId4 =
        new DistributionLocatorId(distributionLocatorId3.marshal());

    assertThat(distributionLocatorId1).isEqualTo(distributionLocatorId2);
    assertThat(distributionLocatorId1).isEqualTo(distributionLocatorId3);
    assertThat(distributionLocatorId1).isEqualTo(distributionLocatorId4);

    assertThat(distributionLocatorId1.getMemberName()).isEqualTo(DistributionConfig.DEFAULT_NAME);
    assertThat(distributionLocatorId2.getMemberName()).isEqualTo("member2");
    assertThat(distributionLocatorId3.getMemberName()).isEqualTo("member3");
    assertThat(distributionLocatorId4.getMemberName()).isEqualTo(DistributionConfig.DEFAULT_NAME);

    assertThat(distributionLocatorId1.detailCompare(distributionLocatorId3))
        .as("Distribution locator IDs 1 and 3 have all parameters the same.").isTrue();
    assertThat(distributionLocatorId2.detailCompare(distributionLocatorId4))
        .as("Distribution locator IDs 2 and 4 have all parameters the same.").isFalse();
  }

  @Test
  void toStringReturnsMarshaledAddressWhenConstructedWithMarshaledAddress() {
    final DistributionLocatorId locatorId = new DistributionLocatorId("localhost[1234]");

    assertThat(locatorId).hasToString("localhost[1234]");
  }

  @Test
  void toStringReturnsHostnameForClientsWhenConstructedWithHostnameForClients() {
    final DistributionLocatorId locatorId =
        new DistributionLocatorId(1234, "bind-address.example.com",
            "hostname-for-clients.example.com");

    assertThat(locatorId).hasToString("hostname-for-clients.example.com[1234]");
  }

  @Test
  void toStringReturnsBindAddressWhenConstructedWithBindAddress() {
    final DistributionLocatorId locatorId =
        new DistributionLocatorId(1234, "bind-address.example.com");

    assertThat(locatorId).hasToString("bind-address.example.com[1234]");
  }

  @Test
  void toStringReturnsLocalHostNameWhenLocatorBindAddressAndHostnameForClientNotSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final Locator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    assertThat(locatorId).hasToString(localHost.getCanonicalHostName() + "[1234]");
  }

  @Test
  void toStringReturnsBindAddressWhenLocatorBindAddressSetAndHostnameForClientNotSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final InternalLocator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);
    when(locator.getBindAddressString()).thenReturn("bind-address.example.com");

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    assertThat(locatorId).hasToString("bind-address.example.com[1234]");
  }

  @Test
  void toStringReturnsBindAddressWhenLocatorBindAddressAndHostnameForClientSet()
      throws UnknownHostException {
    final InetAddress localHost = LocalHostUtil.getLocalHost();

    final InternalLocator locator = mock(InternalLocator.class);
    when(locator.getPort()).thenReturn(1234);
    when(locator.getBindAddressString()).thenReturn("bind-address.example.com");
    when(locator.getHostnameForClients()).thenReturn("hostname-for-clients.example.com");

    final DistributionLocatorId locatorId = new DistributionLocatorId(localHost, locator);

    assertThat(locatorId).hasToString("hostname-for-clients.example.com[1234]");
  }
}
