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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.internal.cli.functions.GatewaySenderFunctionArgs;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class AlterGatewaySenderCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private AlterGatewaySenderCommand command;
  private InternalConfigurationPersistenceService service;
  private Set<String> groupSet = new HashSet<>();

  private List<CliFunctionResult> functionResults;
  private CliFunctionResult cliFunctionResult;
  private ArgumentCaptor<GatewaySenderFunctionArgs> argsArgumentCaptor =
      ArgumentCaptor.forClass(GatewaySenderFunctionArgs.class);


  @Before
  public void before() {
    command = spy(AlterGatewaySenderCommand.class);
    InternalCache cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
    service =
        spy(new InternalConfigurationPersistenceService(JAXBService.create(CacheConfig.class)));

    doReturn(service).when(command).getConfigurationPersistenceService();

    groupSet.add("group1");
    groupSet.add("group2");
    doReturn(groupSet).when(service).getGroups();

    CacheConfig config = new CacheConfig();
    CacheConfig.GatewaySender gw1 = new CacheConfig.GatewaySender();
    gw1.setId("sender1");
    gw1.setParallel(true);
    gw1.setGroupTransactionEvents(false);
    gw1.setEnableBatchConflation(true);

    CacheConfig.GatewaySender gw2 = new CacheConfig.GatewaySender();
    gw2.setId("sender2");
    gw2.setParallel(false);
    gw2.setDispatcherThreads("5");
    gw2.setGroupTransactionEvents(false);
    gw2.setEnableBatchConflation(false);

    config.getGatewaySenders().add(gw1);
    config.getGatewaySenders().add(gw2);

    doReturn(config).when(service).getCacheConfig("group1");
    doReturn(new CacheConfig()).when(service).getCacheConfig("group2");

    Set<DistributedMember> members =
        Stream.of(mock(DistributedMember.class)).collect(Collectors.toSet());
    doReturn(members).when(command).findMembers(any(), any());

  }

  @Test
  public void mandatoryOption() {
    gfsh.executeAndAssertThat(command, "alter gateway-sender").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void noOptionToModify() {
    gfsh.executeAndAssertThat(command, "alter gateway-sender --id=sender1").statusIsError()
        .containsOutput("Please provide a relevant parameter(s)");
  }

  @Test
  public void unknownOption() {
    gfsh.executeAndAssertThat(command, "alter gateway-sender --id=sender1 --status").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void emptyConfiguration() {
    gfsh.executeAndAssertThat(command, "alter gateway-sender --id=test --batch-size=100")
        .statusIsError().containsOutput("Can not find an gateway sender");
  }

  @Test
  public void changeGroupTransaction1() {
    gfsh.executeAndAssertThat(command,
        "alter gateway-sender --id=sender1 --group-transaction-events").statusIsError()
        .containsOutput("Alter Gateway Sender cannot be performed");
  }

  @Test
  public void changeGroupTransaction2() {
    gfsh.executeAndAssertThat(command,
        "alter gateway-sender --id=sender2 --group-transaction-events").statusIsError()
        .containsOutput("Alter Gateway Sender cannot be performed");
  }

}
