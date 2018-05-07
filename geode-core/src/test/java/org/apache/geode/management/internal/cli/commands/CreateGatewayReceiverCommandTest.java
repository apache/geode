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
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class CreateGatewayReceiverCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateGatewayReceiverCommand command;
  private InternalCache cache;
  private List<CliFunctionResult> functionResults;
  private InternalConfigurationPersistenceService ccService;
  private CliFunctionResult result1;

  @Before
  public void before() {
    command = spy(CreateGatewayReceiverCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(),
        any(Set.class));
  }

  @Test
  public void testDefaultValues() {
    GfshParseResult parseResult = gfsh.parse("create gateway-receiver");

    assertThat(parseResult.getParamValue(CliStrings.MEMBER)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.GROUP)).isNull();
    assertThat(parseResult.getParamValue(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART)).isNull();
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
    result1 = new CliFunctionResult("member", true, "result1");
    functionResults.add(result1);
    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsSuccess()
        .containsOutput("Cluster configuration is not updated");
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  @Test
  public void gatewayReceiverIsCreatedButNotPersistedWithMemberOption() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", true, "result1");
    functionResults.add(result1);
    gfsh.executeAndAssertThat(command, "create gateway-receiver --member=xyz").statusIsSuccess()
        .containsOutput("Cluster configuration is not updated");
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  @Test
  public void configurationIsNotPersistedWhenCreationOnOnlyMemberFails() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", false, "result1");
    functionResults.add(result1);

    // does not delete because command failed, so hasNoFailToPersistError should still be true
    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsError()
        .containsOutput("Cluster configuration is not updated");
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  @Test
  public void configurationIsPersistedWhenCreationOnAnyMemberFails() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    result1 = new CliFunctionResult("member", false, "result1");
    functionResults.add(result1);
    CliFunctionResult result2 = new CliFunctionResult("member", true, "result2");
    functionResults.add(result2);

    // does not delete because command failed, so hasNoFailToPersistError should still be true
    gfsh.executeAndAssertThat(command, "create gateway-receiver").statusIsSuccess()
        .doesNotContainOutput("Cluster configuration is not updated");
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }
}
