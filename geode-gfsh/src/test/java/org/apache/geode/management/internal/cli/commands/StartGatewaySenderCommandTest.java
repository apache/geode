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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StartGatewaySenderCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private StartGatewaySenderCommand command;
  private InternalConfigurationPersistenceService service;
  private final Set<String> groupSet = new HashSet<>();
  private Set<DistributedMember> memberSet;
  private DistributedMember distributedMember1;
  private DistributedMember distributedMember2;
  private Cache cache;
  private DistributedSystem system;
  private DistributedSystem locatorSystem;
  private DistributedMember locatorDistributedMember;
  private SystemManagementService serviceMgm;
  private GatewaySenderMXBean bean1;
  private GatewaySenderMXBean bean2;
  private CacheConfig config;
  private CacheConfig.GatewaySender gs1Config;
  private ObjectName objectName1;
  private ObjectName objectName2;
  private static final String GATEWAY_SENDER_1 = "gatewaySender1";
  private static final String MEMBER_1 = "member1";
  private static final String MEMBER_2 = "member2";

  @Before
  public void setUp() {
    command = spy(StartGatewaySenderCommand.class);
    service =
        spy(new InternalConfigurationPersistenceService(JAXBService.create(CacheConfig.class)));
    Region configRegion = mock(AbstractRegion.class);
    cache = mock(Cache.class);
    system = mock(DistributedSystem.class);
    locatorSystem = mock(DistributedSystem.class);
    locatorDistributedMember = mock(DistributedMember.class);
    distributedMember1 = mock(DistributedMember.class);
    distributedMember2 = mock(DistributedMember.class);
    serviceMgm = mock(SystemManagementService.class);
    bean1 = mock(GatewaySenderMXBean.class);
    bean2 = mock(GatewaySenderMXBean.class);
    objectName1 = mock(ObjectName.class);
    objectName2 = mock(ObjectName.class);

    doReturn(service).when(command).getConfigurationPersistenceService();
    doReturn(cache).when(command).getCache();
    doReturn(serviceMgm).when(command).getManagementService();

    doReturn(true).when(service).lockSharedConfiguration();
    doNothing().when(service).unlockSharedConfiguration();
    doReturn(null).when(service).getConfiguration(any());
    doReturn(configRegion).when(service).getConfigurationRegion();
    doCallRealMethod().when(service).updateCacheConfig(any(), any());

    config = new CacheConfig();
    gs1Config = new CacheConfig.GatewaySender();
    gs1Config.setId(GATEWAY_SENDER_1);

    memberSet = new HashSet<>();
  }

  @Test
  public void mandatoryOption() {
    gfsh.executeAndAssertThat(command, "start gateway-sender").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void startGatewaySenderSuccessfully() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(memberSet).when(command).findMembers(null, null);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(false).when(bean1).isRunning();

    gfsh.executeAndAssertThat(command, "start gateway-sender --id=gatewaySender1").statusIsSuccess()
        .containsOutput("GatewaySender " + GATEWAY_SENDER_1 + " is started on member " + MEMBER_1)
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK");
  }

  @Test
  public void startGatewaySenderOnTwoMembersSuccessfully() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(objectName1).when(serviceMgm).getGatewaySenderMBeanName(distributedMember1,
        GATEWAY_SENDER_1);
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);
    // member 2
    when(system.getDistributedMember()).thenReturn(distributedMember2);
    memberSet.add(distributedMember2);
    doReturn(MEMBER_2).when(distributedMember2).getId();
    doReturn(objectName2).when(serviceMgm).getGatewaySenderMBeanName(distributedMember2,
        GATEWAY_SENDER_1);
    doReturn(bean2).when(serviceMgm).getMBeanProxy(any(), any());

    // setup for both members
    doReturn(memberSet).when(command).findMembers(null, null);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(false).when(bean1).isRunning();
    doReturn(false).when(bean2).isRunning();

    gfsh.executeAndAssertThat(command, "start gateway-sender --id=gatewaySender1").statusIsSuccess()
        .containsOutput("GatewaySender " + GATEWAY_SENDER_1 + " is started on member " + MEMBER_1)
        .containsOutput("GatewaySender " + GATEWAY_SENDER_1 + " is started on member " + MEMBER_2)
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");
  }

  @Test
  public void startGatewaySenderOnOneMembersSuccessfullyAndOneFail() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(objectName1).when(serviceMgm).getGatewaySenderMBeanName(distributedMember1,
        GATEWAY_SENDER_1);
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);
    // member 2
    when(system.getDistributedMember()).thenReturn(distributedMember2);
    memberSet.add(distributedMember2);
    doReturn(MEMBER_2).when(distributedMember2).getId();
    doReturn(objectName2).when(serviceMgm).getGatewaySenderMBeanName(distributedMember2,
        GATEWAY_SENDER_1);
    doReturn(null).when(serviceMgm).getMBeanProxy(objectName2, GatewaySenderMXBean.class);

    // setup for both members
    doReturn(memberSet).when(command).findMembers(null, null);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(false).when(bean1).isRunning();

    gfsh.executeAndAssertThat(command, "start gateway-sender --id=gatewaySender1").statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .containsOutput(
            "GatewaySender " + GATEWAY_SENDER_1 + " is not available on member " + MEMBER_2)
        .containsOutput("GatewaySender " + GATEWAY_SENDER_1 + " is started on member " + MEMBER_1)
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("Error", "OK");
  }

  @Test
  public void startGatewaySenderTwoMemberCleanQueues() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(objectName1).when(serviceMgm).getGatewaySenderMBeanName(distributedMember1,
        GATEWAY_SENDER_1);
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);
    // member 2
    when(system.getDistributedMember()).thenReturn(distributedMember2);
    memberSet.add(distributedMember2);
    doReturn(MEMBER_2).when(distributedMember2).getId();
    doReturn(objectName2).when(serviceMgm).getGatewaySenderMBeanName(distributedMember2,
        GATEWAY_SENDER_1);
    doReturn(bean2).when(serviceMgm).getMBeanProxy(any(), any());

    // setup for both members
    doReturn(memberSet).when(command).findMembers(null, null);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(false).when(bean1).isRunning();
    doReturn(false).when(bean2).isRunning();

    gfsh.executeAndAssertThat(command,
        "start gateway-sender --id=gatewaySender1 --clean-queues=true").statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");
  }

  @Test
  public void memberAlreadyStarted() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(memberSet).when(command).findMembers(null, null);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(true).when(bean1).isRunning();

    gfsh.executeAndAssertThat(command, "start gateway-sender --id=gatewaySender1").statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
        .containsOutput(
            "GatewaySender " + GATEWAY_SENDER_1 + " is already started on member " + MEMBER_1)
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("Error");
  }

  @Test
  public void memberIsNotAvailable() {
    doReturn(locatorSystem).when(cache).getDistributedSystem();
    doReturn(locatorDistributedMember).when(locatorSystem).getDistributedMember();
    doReturn("locator").when(locatorDistributedMember).getId();

    // member 1
    when(system.getDistributedMember()).thenReturn(distributedMember1);
    memberSet.add(distributedMember1);
    doReturn(memberSet).when(command).findMembers(null, null);
    doReturn(MEMBER_1).when(distributedMember1).getId();
    doReturn(bean1).when(serviceMgm).getMBeanProxy(any(), any());
    config.getGatewaySenders().add(gs1Config);

    groupSet.add("cluster");
    doReturn(groupSet).when(service).getGroups();
    doReturn(config).when(service).getCacheConfig("cluster");
    doReturn(config).when(service).getCacheConfig("cluster", true);

    doReturn(true).when(bean1).isRunning();

    gfsh.executeAndAssertThat(command, "start gateway-sender --id=gatewaySender1").statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("Error");
  }
}
