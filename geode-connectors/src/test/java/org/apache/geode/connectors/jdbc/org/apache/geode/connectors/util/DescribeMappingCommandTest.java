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
package org.apache.geode.connectors.jdbc.org.apache.geode.connectors.util;

import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.cli.DescribeMappingCommand;
import org.apache.geode.connectors.util.internal.DescribeMappingResult;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class DescribeMappingCommandTest {
  public static final String COMMAND = "describe jdbc-mapping --region=region ";
  private DescribeMappingCommand command;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(DescribeMappingCommand.class);
  }

  @Test
  public void requiredParameter() {
    gfsh.executeAndAssertThat(command, "describe jdbc-mapping").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void whenNoMemberExists() {
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void whenMemberExists() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    Map<String, String> attributes = new HashMap<>();
    attributes.put(REGION_NAME, "region");
    attributes.put(PDX_NAME, "class1");
    attributes.put(TABLE_NAME, "table1");
    attributes.put(DATA_SOURCE_NAME, "name1");

    DescribeMappingResult mappingResult = new DescribeMappingResult(attributes);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(
        Collections.singletonList(new CliFunctionResult("server-1", mappingResult, "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("region", "region")
        .containsOutput("data-source", "name1")
        .containsOutput("table", "table1")
        .containsOutput("pdx-name", "class1")
        .containsOutput("id", "myId");
  }

  @Test
  public void whenMemberExistsWithRegionPathFunctionIsCalledWithNoSlashRegionName() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);
    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(
        Collections.singletonList(
            new CliFunctionResult("server-1", mock(DescribeMappingResult.class), "success")));

    command.describeMapping("/regionName");

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(), eq("regionName"), any());
  }

  @Test
  public void whenNoMappingFoundOnMember() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.emptyList());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("mapping for region 'region' not found");
  }
}
