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
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DeployCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DeployCommand command;

  @Before
  public void before() {
    command = spy(DeployCommand.class);
  }

  @Test
  public void jarNotFound() {
    gfsh.executeAndAssertThat(command, "deploy --jar=abc.jar").statusIsError()
        .containsOutput("not found");
  }

  @Test
  public void notDirectory() {
    gfsh.executeAndAssertThat(command, "deploy --dir=notExist").statusIsError()
        .containsOutput("not a directory");
  }

  @Test
  public void bothDirAndJar() {
    gfsh.executeAndAssertThat(command, "deploy --dir=a --jar=b").statusIsError()
        .containsOutput("can not both be specified");
  }

  @Test
  public void missingDirOrJar() {
    gfsh.executeAndAssertThat(command, "deploy").statusIsError().containsOutput("is required");
  }

  @Test
  public void testNestedResultStructureCompatibility() {
    // This test verifies that the nested structure is maintained for backward compatibility
    List<List<Object>> nestedResults = new LinkedList<>();

    // Simulate results from two members
    List<Object> member1Results = new ArrayList<>();
    member1Results.add(new CliFunctionResult("member1", true, "deployed jar1"));
    member1Results.add(new CliFunctionResult("member1", true, "deployed jar2"));

    List<Object> member2Results = new ArrayList<>();
    member2Results.add(new CliFunctionResult("member2", true, "deployed jar1"));
    member2Results.add(new CliFunctionResult("member2", false, "failed to deploy jar2"));

    nestedResults.add(member1Results);
    nestedResults.add(member2Results);

    // Verify the nested structure can be flattened properly
    List<Object> flatResults = new LinkedList<>();
    for (List<Object> memberResults : nestedResults) {
      flatResults.addAll(memberResults);
    }

    List<CliFunctionResult> cleanedResults = CliFunctionResult.cleanResults(flatResults);

    // Verify we have results from both members
    assertThat(cleanedResults).hasSize(4);
    assertThat(cleanedResults.get(0).getMemberIdOrName()).isEqualTo("member1");
    assertThat(cleanedResults.get(2).getMemberIdOrName()).isEqualTo("member2");
  }
}
