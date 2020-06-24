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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class CreateGatewayReceiverCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateGatewayReceiverCommand command;
  private List<CliFunctionResult> functionResults;
  private InternalConfigurationPersistenceService ccService;
  private CliFunctionResult result1;

  @Before
  public void before() {
    command = spy(CreateGatewayReceiverCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(cache.getInternalDistributedSystem())
        .thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getModuleService())
        .thenReturn(new ServiceLoaderModuleService(LogService.getLogger()));
    doReturn(cache).when(command).getCache();
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void testUnspecifiedDefaultValues() {
    GfshParseResult parseResult = gfsh.parse("create gateway-receiver");

    assertThat(parseResult.getParamValue(CliStrings.MEMBER)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.GROUP)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.IFNOTEXISTS)).isEqualTo(false);
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART))
        .isEqualTo(false);
  }

  @Test
  public void testSpecifiedDefaultValues() {
    GfshParseResult parseResult =
        gfsh.parse("create gateway-receiver --manual-start --if-not-exists");

    assertThat(parseResult.getParamValue(CliStrings.MEMBER)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.GROUP)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS))
        .isNull();
    assertThat(parseResult.getParamValue(CliStrings.IFNOTEXISTS)).isEqualTo(true);
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART))
        .isEqualTo(true);
  }

  @Test
  public void endPortMustBeLargerThanStartPort() {
    gfsh.executeAndAssertThat(command, "create gateway-receiver --end-port=1").statusIsError()
        .containsOutput("start-port must be smaller than end-port");

    gfsh.executeAndAssertThat(command, "create gateway-receiver --start-port=60000").statusIsError()
        .containsOutput("start-port must be smaller than end-port");

    gfsh.executeAndAssertThat(command, "create gateway-receiver --end-port=1 --start-port=2")
        .statusIsError().containsOutput("start-port must be smaller than end-port");
  }

  @Test
  public void gatewayReceiverCanBeCreatedButIsNotPersistedWithoutConfigurationService() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", CliFunctionResult.StatusState.OK, "result1");
    functionResults.add(result1);

    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsSuccess()
        .containsOutput(
            "Cluster configuration service is not running. Configuration change is not persisted");
    verify(ccService, never()).addXmlEntity(any(), any());
    verify(ccService, never()).updateCacheConfig(any(), any());
  }

  @Test
  public void gatewayReceiverIsCreatedButNotPersistedWithMemberOption() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", CliFunctionResult.StatusState.OK, "result1");
    functionResults.add(result1);
    gfsh.executeAndAssertThat(command, "create gateway-receiver --member=xyz").statusIsSuccess()
        .containsOutput(
            "Configuration change is not persisted because the command is executed on specific member");
    verify(ccService, never()).addXmlEntity(any(), any());
    verify(ccService, never()).updateCacheConfig(any(), any());
  }

  @Test
  public void configurationIsNotPersistedWhenCreationOnOnlyMemberFails() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", CliFunctionResult.StatusState.ERROR, "result1");
    functionResults.add(result1);

    // does not delete because command failed, so hasNoFailToPersistError should still be true
    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsError();
    verify(ccService, never()).updateCacheConfig(any(), any());
  }

  @Test
  public void configurationIsPersistedWhenCreationOnAnyMemberFails() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", CliFunctionResult.StatusState.ERROR, "result1");
    functionResults.add(result1);
    CliFunctionResult result2 =
        new CliFunctionResult("member", CliFunctionResult.StatusState.OK, "result2");
    functionResults.add(result2);

    // does not delete because command failed, so hasNoFailToPersistError should still be true
    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsSuccess();
    verify(ccService, times(1)).updateCacheConfig(any(), any());
  }
}
