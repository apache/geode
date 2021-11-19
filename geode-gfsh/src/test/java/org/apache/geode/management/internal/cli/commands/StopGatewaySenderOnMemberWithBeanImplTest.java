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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.StopGatewaySenderCommandDelegateParallelImpl.StopGatewaySenderOnMember;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class StopGatewaySenderOnMemberWithBeanImplTest {
  String senderId = "sender1";
  String memberId = "member1";
  String remoteMemberId = "remoteMember1";
  Cache cache;
  SystemManagementService managementService;
  DistributedMember distributedMember;
  DistributedMember remoteDistributedMember;
  DistributedSystem distributedSystem;

  @Before
  public void setUp() {
    cache = mock(Cache.class);
    managementService = mock(SystemManagementService.class);
    distributedMember = mock(DistributedMember.class);
    remoteDistributedMember = mock(DistributedMember.class);
    when(distributedMember.getId()).thenReturn(memberId);
    when(remoteDistributedMember.getId()).thenReturn(remoteMemberId);
    distributedSystem = mock(DistributedSystem.class);
    doReturn(distributedSystem).when(cache).getDistributedSystem();
  }

  @Test
  @Parameters({"true", "false"})
  public void executeStopGatewaySenderOnMemberNotRunningReturnsNotRunningError(
      boolean isLocalMember) {
    GatewaySenderMXBean gatewaySenderMXBean = gatewaySenderMXBean(isLocalMember, false);
    when(gatewaySenderMXBean.isRunning()).thenReturn(false);

    StopGatewaySenderOnMember stopperWithBean = new StopGatewaySenderOnMemberWithBeanImpl();

    List<String> result = stopperWithBean.executeStopGatewaySenderOnMember(senderId, cache,
        managementService, distributedMember);
    assertThat(result).containsExactly(memberId, "Error",
        "GatewaySender sender1 is not running on member " + memberId + ".");
  }

  @Test
  @Parameters({"true", "false"})
  public void executeStopGatewaySenderOnMemberNotAvailableReturnsNotAvailableError(
      boolean isLocalMember) {
    gatewaySenderMXBean(isLocalMember, true);

    StopGatewaySenderOnMember stopperWithBean = new StopGatewaySenderOnMemberWithBeanImpl();

    List<String> result = stopperWithBean.executeStopGatewaySenderOnMember(senderId, cache,
        managementService, distributedMember);

    assertThat(result).containsExactly(memberId, "Error",
        "GatewaySender sender1 is not available on member " + memberId);
  }

  @Test
  @Parameters({"true", "false"})
  public void executeStopGatewaySenderOnMemberRunningReturnsOk(boolean isLocalMember) {
    GatewaySenderMXBean gatewaySenderMXBean = gatewaySenderMXBean(isLocalMember, false);
    when(gatewaySenderMXBean.isRunning()).thenReturn(true);

    StopGatewaySenderOnMember stopperWithBean = new StopGatewaySenderOnMemberWithBeanImpl();

    List<String> result = stopperWithBean.executeStopGatewaySenderOnMember(senderId, cache,
        managementService, distributedMember);

    assertThat(result).containsExactly(memberId, "OK",
        "GatewaySender sender1 is stopped on member " + memberId);
  }

  private GatewaySenderMXBean gatewaySenderMXBean(boolean isLocalMember, boolean mustBeNull) {
    GatewaySenderMXBean gatewaySenderMXBean = mustBeNull
        ? null
        : mock(GatewaySenderMXBean.class);
    if (isLocalMember) {
      doReturn(distributedMember).when(distributedSystem).getDistributedMember();
      doReturn(gatewaySenderMXBean).when(managementService).getLocalGatewaySenderMXBean(senderId);
    } else {
      doReturn(remoteDistributedMember).when(distributedSystem).getDistributedMember();
      doReturn(gatewaySenderMXBean).when(managementService).getMBeanProxy(any(), any());
    }
    return gatewaySenderMXBean;
  }
}
