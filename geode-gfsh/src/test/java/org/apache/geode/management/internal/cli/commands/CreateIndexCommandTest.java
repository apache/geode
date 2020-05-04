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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateIndexCommandTest {
  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private CreateIndexCommand command;
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
    assertThat(command.getValidRegionName("regionB")).isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionB")).isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionB b")).isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionB.entrySet()"))
        .isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionA.B.entrySet() A"))
        .isEqualTo("regionA");
    assertThat(command.getValidRegionName("/regionA.fieldName.entrySet() B"))
        .isEqualTo("regionA");
  }

  @Test
  public void commandWithGroup() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    doReturn(Sets.newHashSet("group1", "group2")).when(command).getGroupsContainingRegion(any(),
        any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());

    CliFunctionResult result = new CliFunctionResult("member", true, "reason");
    doReturn(Collections.singletonList(result)).when(command).executeAndGetFunctionResult(any(),
        any(), any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA --groups=group1,group2")
        .statusIsSuccess();

    verify(ccService).updateCacheConfig(eq("group1"), any());
    verify(ccService).updateCacheConfig(eq("group2"), any());
  }

  @Test
  public void commandWithWrongGroup() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    doReturn(Sets.newHashSet("group1", "group2")).when(command).getGroupsContainingRegion(any(),
        any());

    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA --groups=group1,group3")
        .statusIsError()
        .containsOutput("Region regionA does not exist in some of the groups");

    verify(ccService, never()).updateCacheConfig(any(), any());
  }

  @Test
  public void csServiceIsDisabled() throws Exception {
    doReturn(null).when(command).getConfigurationPersistenceService();
    Set<DistributedMember> targetMembers = Collections.singleton(mock(DistributedMember.class));
    doReturn(targetMembers).when(command).findMembers(any(),
        any());
    CliFunctionResult result = new CliFunctionResult("member", true, "result:xyz");
    doReturn(Collections.singletonList(result)).when(command).executeAndGetFunctionResult(any(),
        any(), any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA")
        .statusIsSuccess()
        .containsOutput("result:xyz")
        .containsOutput(
            "Cluster configuration service is not running. Configuration change is not persisted.");

    verify(command).executeAndGetFunctionResult(any(), any(), eq(targetMembers));
  }

  @Test
  public void commandWithMember() throws Exception {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    Set<DistributedMember> targetMembers = Collections.singleton(mock(DistributedMember.class));
    doReturn(targetMembers).when(command).findMembers(any(), any());
    CliFunctionResult result = new CliFunctionResult("member", true, "result:xyz");
    doReturn(Collections.singletonList(result)).when(command).executeAndGetFunctionResult(any(),
        any(), any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA --member=member")
        .statusIsSuccess()
        .containsOutput("result:xyz")
        .containsOutput(
            "Configuration change is not persisted because the command is executed on specific member.");

    verify(ccService, never()).updateCacheConfig(any(), any());
  }

  @Test
  public void regionBelongsToCluster() throws Exception {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    Region region = mock(Region.class);
    ClusterManagementListResult listResult = mock(ClusterManagementListResult.class);
    when(cms.list(any(Region.class))).thenReturn(listResult);
    when(listResult.getConfigResult()).thenReturn(Collections.singletonList(region));

    doReturn(Sets.newHashSet("cluster")).when(command).getGroupsContainingRegion(any(),
        any());
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());

    gfshParser.executeAndAssertThat(command,
        "create index --name=index --expression=abc --region=/regionA")
        .containsOutput("No Members Found");

    verify(command).findMembers(new String[] {}, null);
  }

  @Test
  public void getGroupsContainingRegion() throws Exception {
    when(ccService.getGroups()).thenReturn(new HashSet<>(Arrays.asList("group1", "group2")));
    CacheConfig cacheConfig1 = new CacheConfig();
    RegionConfig regionA = new RegionConfig("regionA", "REPLICATE");
    RegionConfig regionB = new RegionConfig("regionB", "REPLICATE");
    RegionConfig child = new RegionConfig("child", "REPLICATE");
    regionA.getRegions().add(child);
    cacheConfig1.getRegions().add(regionA);
    cacheConfig1.getRegions().add(regionB);
    CacheConfig cacheConfig2 = new CacheConfig();
    cacheConfig2.getRegions().add(regionB);

    when(ccService.getCacheConfig("group1", true)).thenReturn(cacheConfig1);
    when(ccService.getCacheConfig("group2", true)).thenReturn(cacheConfig2);

    assertThat(command.getGroupsContainingRegion(ccService, "regionA"))
        .containsExactly("group1");
    assertThat(command.getGroupsContainingRegion(ccService, "regionB"))
        .containsExactlyInAnyOrder("group1", "group2");
    assertThat(command.getGroupsContainingRegion(ccService, "regionA/child"))
        .containsExactly("group1");
    assertThat(command.getGroupsContainingRegion(ccService, "notExist"))
        .isEmpty();
  }
}
