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
package org.apache.geode.distributed.internal;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.inet.LocalHostUtil;

public class ClusterDistributionManagerTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Test
  public void membershipFailureProcessingCreatesForcedDisconnectException() {
    ClusterDistributionManager manager = mock(ClusterDistributionManager.class);
    ClusterDistributionManager.DMListener listener =
        new ClusterDistributionManager.DMListener(manager);
    listener.membershipFailure("Testing", new MemberDisconnectedException("testing"));
    // the root cause of membership failure should only be set once
    verify(manager, times(1)).setRootCause(isA(Throwable.class));
    // the root cause should be a ForcedDisconnectException
    verify(manager, times(1)).setRootCause(isA(ForcedDisconnectException.class));
  }

  @Test
  public void getEquivalentsForLocalHostReturnsOneAddress() throws UnknownHostException {
    AlertingService alertingService = mock(AlertingService.class);
    Distribution distribution = mock(Distribution.class);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    InetAddress localHost = LocalHostUtil.getLocalHost();
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    MembershipLocator<InternalDistributedMember> membershipLocator =
        uncheckedCast(mock(MembershipLocator.class));
    RemoteTransportConfig transportConfig = mock(RemoteTransportConfig.class);

    when(distribution.getView()).thenReturn(uncheckedCast(mock(MembershipView.class)));
    when(system.getConfig()).thenReturn(distributionConfig);

    DistributionManager distributionManager = new ClusterDistributionManager(
        system,
        transportConfig,
        alertingService,
        membershipLocator,
        (sys, statId) -> mock(DistributionStats.class),
        (dm, transport, sys, membershipListener, messageListener, locator) -> distribution);

    Set<InetAddress> members = distributionManager.getEquivalents(localHost);

    assertThat(members).contains(localHost);
  }
}
