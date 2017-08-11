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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StartServerCommandUnitTest {
  @Rule
  public GfshParserRule gfshRule = new GfshParserRule();

  private StartServerCommand command;
  private CommandResult result;

  @Before
  public void before() throws Exception {
    command = spy(StartServerCommand.class);
    Gfsh gfsh = mock(Gfsh.class);
    when(command.getGfsh()).thenReturn(gfsh);
  }

  @Test
  public void failsFastOnMissingCacheXmlFile() throws Exception {
    result = gfshRule.executeCommandWithInstance(command, "start server --cache-xml-file=notExist");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.toString()).contains("notExist could not be found.");
  }

  @Test
  public void failsFastOnMissingGemFirePropertiesFile() throws Exception {
    result =
        gfshRule.executeCommandWithInstance(command, "start server --properties-file=notExist");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.toString()).contains("notExist could not be found.");
  }

  @Test
  public void failsFastOnMissingGemFireSecurityPropertiesFile() throws Exception {
    result = gfshRule.executeCommandWithInstance(command,
        "start server --security-properties-file=notExist");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.toString()).contains("notExist could not be found.");
  }

  @Test
  public void failsFastOnMissingPassword() throws Exception {
    result = gfshRule.executeCommandWithInstance(command, "start server --user=test");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.toString()).contains("password must be specified.");
  }
}
