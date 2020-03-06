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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateDefinedIndexesCommandTest {
  @SuppressWarnings("deprecation")
  private static final org.apache.geode.cache.query.IndexType FUNCTIONAL =
      org.apache.geode.cache.query.IndexType.FUNCTIONAL;

  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private ResultCollector<?, ?> resultCollector;
  private CreateDefinedIndexesCommand command;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    IndexDefinition.indexDefinitions.clear();
    resultCollector = mock(ResultCollector.class);
    command = spy(CreateDefinedIndexesCommand.class);
    doReturn(resultCollector).when(command).executeFunction(any(), any(), any(Set.class));
  }

  @Test
  public void noDefinitions() {
    gfshParser.executeAndAssertThat(command, "create defined indexes")
        .statusIsSuccess()
        .containsOutput("No indexes defined");
  }

  @Test
  public void noMembers() {
    IndexDefinition.addIndex("indexName", "indexedExpression", "TestRegion", FUNCTIONAL);
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    gfshParser.executeAndAssertThat(command, "create defined indexes")
        .statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void creationFailure() {
    DistributedMember member = mock(DistributedMember.class);
    when(member.getId()).thenReturn("memberId");
    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());
    doReturn(Collections
        .singletonList(new CliFunctionResult(member.getId(), new Exception("MockException"),
            "Exception Message."))).when(resultCollector).getResult();

    IndexDefinition.addIndex("index1", "value1", "TestRegion", FUNCTIONAL);
    gfshParser.executeAndAssertThat(command, "create defined indexes").statusIsError();
  }

  @Test
  public void creationSuccess() {
    DistributedMember member = mock(DistributedMember.class);
    when(member.getId()).thenReturn("memberId");
    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());
    List<String> indexes = new ArrayList<>();
    indexes.add("index1");
    doReturn(Collections.singletonList(new CliFunctionResult(member.getId(), indexes)))
        .when(resultCollector)
        .getResult();

    IndexDefinition.addIndex("index1", "value1", "TestRegion", FUNCTIONAL);
    gfshParser.executeAndAssertThat(command, "create defined indexes").statusIsSuccess();

    verify(command, Mockito.times(0)).updateConfigForGroup(any(), any(), any());
  }

  @Test
  public void multipleIndexesOnMultipleRegions() {
    DistributedMember member1 = mock(DistributedMember.class);
    DistributedMember member2 = mock(DistributedMember.class);
    when(member1.getId()).thenReturn("memberId_1");
    when(member2.getId()).thenReturn("memberId_2");

    InternalConfigurationPersistenceService mockService =
        mock(InternalConfigurationPersistenceService.class);
    CliFunctionResult resultMember1 =
        new CliFunctionResult(member1.getId(), Arrays.asList("index1", "index2"));
    CliFunctionResult resultMember2 =
        new CliFunctionResult(member2.getId(), Arrays.asList("index1", "index2"));

    doReturn(mockService).when(command).getConfigurationPersistenceService();
    doReturn(new HashSet<>(Arrays.asList(member1, member2))).when(command).findMembers(any(),
        any());
    doReturn(Arrays.asList(resultMember1, resultMember2)).when(resultCollector).getResult();

    IndexDefinition.addIndex("index1", "value1", "TestRegion1", FUNCTIONAL);
    IndexDefinition.addIndex("index2", "value2", "TestRegion2", FUNCTIONAL);

    gfshParser.executeAndAssertThat(command, "create defined indexes").statusIsSuccess()
        .hasTableSection()
        .hasColumn("Status")
        .containsExactly("OK", "OK");
  }
}
