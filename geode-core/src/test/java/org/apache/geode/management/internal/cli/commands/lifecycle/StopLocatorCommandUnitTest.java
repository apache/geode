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

package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StopLocatorCommandUnitTest {
  @Rule
  public GfshParserRule gfshRule = new GfshParserRule();

  private StopLocatorCommand stopLocatorCommand;
  private CommandResult result;
  private Gfsh gfsh;

  @Before
  public void before() throws Exception {
    stopLocatorCommand = spy(StopLocatorCommand.class);
    gfsh = mock(Gfsh.class);
  }

  @Test
  public void usesMemberNameIDWhenNotConnected() {
    when(stopLocatorCommand.isConnectedAndReady()).thenReturn(false);
    result =
        gfshRule.executeCommandWithInstance(stopLocatorCommand, "stop locator --name=myLocator");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.toString()).contains(
        CliStrings.format(CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Locator"));
  }

  @Test
  public void usesMemberNameIDWhenConnected() throws IOException {
    when(stopLocatorCommand.isConnectedAndReady()).thenReturn(true);
    when(gfsh.isConnectedAndReady()).thenReturn(true);
    doReturn(null).when(stopLocatorCommand).getMemberMXBean(anyString());
    result = gfshRule.executeCommandWithInstance(stopLocatorCommand,
        "stop locator --name=invalidLocator");
    assertThat(result.toString()).contains(CliStrings.format(
        CliStrings.STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, "invalidLocator"));
  }
}
