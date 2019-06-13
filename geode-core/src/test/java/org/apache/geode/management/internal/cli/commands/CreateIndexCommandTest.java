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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateIndexCommandTest {
  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private CreateIndexCommand command;
  private CommandResult result;
  private ResultCollector rc;
  private InternalConfigurationPersistenceService ccService;
  private ClusterManagementService cms;

  @Before
  public void before() throws Exception {
    command = spy(CreateIndexCommand.class);
    rc = mock(ResultCollector.class);
    when(rc.getResult()).thenReturn(Collections.emptyList());
    doReturn(Collections.emptyList()).when(command).executeAndGetFunctionResult(any(), any(),
        any());
    ccService = mock(InternalConfigurationPersistenceService.class);
    cms = mock(ClusterManagementService.class);
  }

  @Test
  public void missingName() throws Exception {
    gfshParser.executeAndAssertThat(command,
        "create index --expression=abc --region=abc")
        .statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void missingExpression() throws Exception {
    gfshParser.executeAndAssertThat(command, "create index --name=abc --region=abc")
        .statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void missingRegion() throws Exception {
    gfshParser.executeAndAssertThat(command, "create index --name=abc --expression=abc")
        .statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void invalidIndexType() throws Exception {
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc --type=abc")
        .statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void validIndexType() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc --type=range")
        .statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void validRegionPath() throws Exception {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=\"region.entrySet() z\" --type=range")
        .statusIsError();
  }

  @Test
  public void validIndexType2() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc --type=hash")
        .statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void noMemberFound() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc")
        .statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void defaultIndexType() throws Exception {
    DistributedMember member = mock(DistributedMember.class);
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());

    ArgumentCaptor<RegionConfig.Index> indexTypeCaptor =
        ArgumentCaptor.forClass(RegionConfig.Index.class);
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc");

    verify(command).executeAndGetFunctionResult(any(), indexTypeCaptor.capture(),
        eq(Collections.singleton(member)));

    assertThat(indexTypeCaptor.getValue().getType()).isEqualTo("range");
  }

  @Test
  public void getValidRegionName() {
    // the existing configuration has a region named /regionA.B
    doReturn(mock(RuntimeRegionConfig.class)).when(command).getRuntimeRegionConfig(cms,
        "/regionA.B");
    when(cms.list(any())).thenReturn(new ClusterManagementResult<>());

    assertThat(command.getValidRegionName("regionB", cms)).isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionB", cms)).isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionB b", cms)).isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionB.entrySet()", cms))
        .isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionA.B.entrySet() A", cms))
        .isEqualTo("/regionA.B");
    assertThat(command.getValidRegionName("/regionA.fieldName.entrySet() B", cms))
        .isEqualTo("/regionA");
  }

  @Test
  public void groupIgnored() throws Exception {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    RuntimeRegionConfig config = mock(RuntimeRegionConfig.class);
    List<String> realGroups = Arrays.asList("group2", "group1");
    when(config.getGroups()).thenReturn(realGroups);
    doReturn(config).when(command).getRuntimeRegionConfig(any(), any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());

    CliFunctionResult result = new CliFunctionResult("member", false, "reason");
    doReturn(Collections.singletonList(result)).when(command).executeAndGetFunctionResult(any(),
        any(), any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA --groups=group1,group2")
        .statusIsError()
        .doesNotContainOutput("--groups=group1,group2 is ignored");

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA --groups=group1,group3")
        .statusIsError()
        .containsOutput("--groups=group1,group3 is ignored");
  }
}
