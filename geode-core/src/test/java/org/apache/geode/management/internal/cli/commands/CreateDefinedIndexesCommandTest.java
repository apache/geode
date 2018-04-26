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

import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.apache.geode.management.cli.Result.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.cli.domain.IndexInfo;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class CreateDefinedIndexesCommandTest {
  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private CommandResult result;
  private ResultCollector resultCollector;
  private CreateDefinedIndexesCommand command;

  @Before
  public void setUp() throws Exception {
    IndexDefinition.indexDefinitions.clear();
    resultCollector = mock(ResultCollector.class);
    command = spy(CreateDefinedIndexesCommand.class);
    doReturn(resultCollector).when(command).executeFunction(any(), any(), any(Set.class));
  }

  @Test
  public void noDefinitions() throws Exception {
    result = gfshParser.executeCommandWithInstance(command, "create defined indexes");
    assertThat(result.getStatus()).isEqualTo(OK);
    assertThat(result.getMessageFromContent()).contains("No indexes defined");
  }

  @Test
  public void noMembers() throws Exception {
    IndexDefinition.indexDefinitions
        .add(new IndexInfo("indexName", "indexedExpression", "TestRegion", IndexType.FUNCTIONAL));
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    result = gfshParser.executeCommandWithInstance(command, "create defined indexes");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("No Members Found");
  }

  @Test
  public void creationFailure() throws Exception {
    DistributedMember member = mock(DistributedMember.class);
    when(member.getId()).thenReturn("memberId");
    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());
    doReturn(Arrays.asList(new CliFunctionResult(member.getId(), new Exception("MockException"),
        "Exception Message."))).when(resultCollector).getResult();

    IndexDefinition.indexDefinitions
        .add(new IndexInfo("index1", "value1", "TestRegion", IndexType.FUNCTIONAL));
    result = gfshParser.executeCommandWithInstance(command, "create defined indexes");

    assertThat(result.getStatus()).isEqualTo(ERROR);
    verify(command, never()).persistClusterConfiguration(any(), any());
  }

  @Test
  public void creationSuccess() throws Exception {
    XmlEntity xmlEntity = mock(XmlEntity.class);
    DistributedMember member = mock(DistributedMember.class);
    when(member.getId()).thenReturn("memberId");
    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());
    doReturn(Arrays.asList(new CliFunctionResult(member.getId(), xmlEntity))).when(resultCollector)
        .getResult();

    IndexDefinition.indexDefinitions
        .add(new IndexInfo("index1", "value1", "TestRegion", IndexType.FUNCTIONAL));
    result = gfshParser.executeCommandWithInstance(command, "create defined indexes");

    assertThat(result.getStatus()).isEqualTo(OK);
    assertThat(result.failedToPersist()).isFalse();
    verify(command, Mockito.times(1)).persistClusterConfiguration(any(), any());
    assertThat(result.getMessageFromContent()).contains("Indexes successfully created");
  }

  @Test
  public void multipleIndexesOnMultipleRegions() throws Exception {
    XmlEntity xmlEntityRegion1 = mock(XmlEntity.class);
    XmlEntity xmlEntityRegion2 = mock(XmlEntity.class);
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    when(member1.getId()).thenReturn("memberId_1");
    when(member2.getId()).thenReturn("memberId_2");

    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);
    CliFunctionResult member1Region1Result =
        new CliFunctionResult(member1.getId(), xmlEntityRegion1);
    CliFunctionResult member1Region2Result =
        new CliFunctionResult(member1.getId(), xmlEntityRegion2);
    CliFunctionResult member2Region1Result =
        new CliFunctionResult(member2.getId(), xmlEntityRegion2);
    CliFunctionResult member2Region2Result =
        new CliFunctionResult(member2.getId(), xmlEntityRegion2);

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(new HashSet<>(Arrays.asList(new DistributedMember[] {member1, member2}))).when(command)
        .findMembers(any(), any());
    doReturn(Arrays.asList(new CliFunctionResult[] {member1Region1Result, member1Region2Result,
        member2Region1Result, member2Region2Result})).when(resultCollector).getResult();

    IndexDefinition.indexDefinitions
        .add(new IndexInfo("index1", "value1", "TestRegion1", IndexType.FUNCTIONAL));
    IndexDefinition.indexDefinitions
        .add(new IndexInfo("index2", "value2", "TestRegion2", IndexType.FUNCTIONAL));

    result = gfshParser.executeCommandWithInstance(command, "create defined indexes");

    assertThat(result.getStatus()).isEqualTo(OK);
    assertThat(result.failedToPersist()).isFalse();

    // The command will receive 4 results from 2 members, but we need to persist only 2 (#regions)
    // of them.
    verify(command, Mockito.times(2)).persistClusterConfiguration(any(), any());
    assertThat(result.getMessageFromContent()).contains("Indexes successfully created");
  }
}
