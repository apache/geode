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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.util.ReflectionUtils;

import java.util.Properties;

public class LauncherLifecycleCommandsTest {
  @ClassRule
  public static GfshParserRule commandRule = new GfshParserRule();

  @Test
  public void getLauncherLifecycleCommand() throws Exception {
    LauncherLifecycleCommands spy = commandRule.spyCommand("start locator");
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
    commandRule.executeLastCommandWithInstance(spy);

    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(), propsCaptor.capture(), any(),
        any(), any(), any(), any());

    Properties properties = propsCaptor.getValue();
    System.out.println();
  }
}
