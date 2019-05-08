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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class UndeployCommandTest {

  private static String COMMAND = "undeploy";

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private UndeployCommand command;

  @Before
  public void before() {
    command = spy(UndeployCommand.class);
  }

  @Test
  public void commandReturnsErrorWhenNoMembersAreAvailable() {
    doReturn(new HashSet()).when(command).findMembers(any(), any());
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
  }

  @Test
  public void commandDisplaysNoJarsFoundWhenNoJarsExist() {
    HashSet memberSet = mock(HashSet.class);
    when(memberSet.isEmpty()).thenReturn(false);
    List<CliFunctionResult> functionResults = new ArrayList<>();
    CliFunctionResult functionResult = mock(CliFunctionResult.class);
    functionResults.add(functionResult);
    Map<String, String> undeployedJars = new HashMap();
    when(functionResult.getResultObject()).thenReturn(undeployedJars);

    doReturn(memberSet).when(command).findMembers(any(), any());
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(), any());
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput(CliStrings.UNDEPLOY__NO_JARS_FOUND_MESSAGE);
  }

  @Test
  public void commandDisplaysCorrectOutputWhenJarIsUndeployedSuccessfully() {
    HashSet memberSet = mock(HashSet.class);
    when(memberSet.isEmpty()).thenReturn(false);
    List<CliFunctionResult> functionResults = new ArrayList<>();
    CliFunctionResult functionResult = mock(CliFunctionResult.class);
    functionResults.add(functionResult);
    Map<String, String> undeployedJars = new HashMap();
    undeployedJars.put("MyTestJar.jar", "MyTestJar.v1.jar");
    when(functionResult.getResultObject()).thenReturn(undeployedJars);
    when(functionResult.isSuccessful()).thenReturn(true);
    when(functionResult.getMemberIdOrName()).thenReturn("server1");

    doReturn(memberSet).when(command).findMembers(any(), any());
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(), any());
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess().hasTableSection().hasRow(0)
        .contains("server1", "MyTestJar.jar", "MyTestJar.v1.jar");
  }
}
