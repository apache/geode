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

import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class ExecuteFunctionCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ExecuteFunctionCommand command;

  @Before
  public void before() throws Exception {
    command = spy(ExecuteFunctionCommand.class);
  }

  @Test
  public void regionAndMember() {
    gfsh.executeAndAssertThat(command, "execute function --id=foo --region=bar --member=baz")
        .statusIsError().containsOutput("Provide Only one of region/member/groups");
  }

  @Test
  public void regionAndGroup() {
    gfsh.executeAndAssertThat(command, "execute function --id=foo --region=bar --group=group1")
        .statusIsError().containsOutput("Provide Only one of region/member/groups");
  }

  @Test
  public void memberAndGroup() {
    gfsh.executeAndAssertThat(command, "execute function --id=foo --member=bar --group=group1")
        .statusIsError().containsOutput("Provide Only one of region/member/groups");
  }


  @Test
  public void filterWithoutRegion() {
    gfsh.executeAndAssertThat(command, "execute function --id=foo --filter=bar").statusIsError()
        .containsOutput("Filters for executing on member or group is not supported");
  }
}
