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
package org.apache.geode.connectors.jdbc.internal.cli;

import static java.util.stream.Collectors.toSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ListMappingCommandTest {
  public static final String COMMAND = "list jdbc-mappings";
  private ListMappingCommand command;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setUp() {
    command = spy(ListMappingCommand.class);
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

    RegionMapping mapping1 =
        new RegionMapping("region1", "class1", "table1", "name1");
    RegionMapping mapping2 =
        new RegionMapping("region2", "class2", "table2", "name2");

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.singletonList(new CliFunctionResult("server-1",
        Stream.of(mapping1, mapping2).collect(toSet()), "success")));

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().containsOutput("region1",
        "region2");
  }


  @Test
  public void whenNoMappingsExist() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(null,
        null);

    ResultCollector rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunction(any(), any(), any(Set.class));
    when(rc.getResult()).thenReturn(Collections.emptyList());

    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("No mappings found");
  }
}
