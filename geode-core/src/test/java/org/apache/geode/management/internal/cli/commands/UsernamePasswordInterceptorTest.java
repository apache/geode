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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class UsernamePasswordInterceptorTest {
  private Gfsh gfsh;
  private GfshParseResult parseResult;
  private UsernamePasswordInterceptor interceptor;

  @Before
  public void setUp() throws Exception {
    gfsh = mock(Gfsh.class);
    parseResult = mock(GfshParseResult.class);
    interceptor = new UsernamePasswordInterceptor(gfsh);
  }

  @Test
  public void WithConnectedGfshWithNoUsernameAndNoPasswordWillPrompt_interactive()
      throws Exception {
    when(gfsh.readText("Username: ")).thenReturn("user");
    when(gfsh.readPassword("Password: ")).thenReturn("pass");
    when(gfsh.isConnectedAndReady()).thenReturn(true);

    when(parseResult.getUserInput()).thenReturn("command");
    when(parseResult.getParamValueAsString("username")).thenReturn("");
    when(parseResult.getParamValueAsString("password")).thenReturn("");

    interceptor.preExecution(parseResult);

    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
    verify(parseResult, times(0)).setUserInput("command");
  }

  @Test
  public void WithConnectedGfshWithUsernameButNoPasswordWillPrompt_interactive()
      throws Exception {
    when(gfsh.readPassword("Password: ")).thenReturn("pass");
    when(gfsh.isConnectedAndReady()).thenReturn(true);

    when(parseResult.getUserInput()).thenReturn("command --username=user");
    when(parseResult.getParamValueAsString("username")).thenReturn("user");
    when(parseResult.getParamValueAsString("password")).thenReturn("");

    interceptor.preExecution(parseResult);

    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(1)).readPassword(any());
    verify(parseResult, times(1)).setUserInput("command --username=user --password=pass");
  }

  @Test
  public void WithConnectedGfshWithUsernameAndPasswordWillNotPrompt_interactive()
      throws Exception {
    when(gfsh.isConnectedAndReady()).thenReturn(true);

    when(parseResult.getUserInput()).thenReturn("command --username=user --password=pass");
    when(parseResult.getParamValueAsString("username")).thenReturn("user");
    when(parseResult.getParamValueAsString("password")).thenReturn("pass");

    interceptor.preExecution(parseResult);

    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
    verify(parseResult, times(0)).setUserInput("command --username=user --password=pass");
  }

  @Test
  public void WithConnectedGfshWithPasswordButNoUsernameWillPrompt_interactive()
      throws Exception {
    when(gfsh.readText("Username: ")).thenReturn("user");
    when(gfsh.isConnectedAndReady()).thenReturn(true);

    when(parseResult.getUserInput()).thenReturn("command --password=pass");
    when(parseResult.getParamValueAsString("username")).thenReturn("");
    when(parseResult.getParamValueAsString("password")).thenReturn("pass");

    interceptor.preExecution(parseResult);

    verify(gfsh, times(1)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
    verify(parseResult, times(1)).setUserInput("command --password=pass --username=user");
  }

  @Test
  public void WithNonConnectedGfshWithoutUsernameAndPasswordWillNotPrompt_interactive()
      throws Exception {
    when(gfsh.isConnectedAndReady()).thenReturn(false);

    when(parseResult.getUserInput()).thenReturn("command");
    when(parseResult.getParamValueAsString("username")).thenReturn("");
    when(parseResult.getParamValueAsString("password")).thenReturn("");

    interceptor.preExecution(parseResult);

    verify(gfsh, times(0)).readText(any());
    verify(gfsh, times(0)).readPassword(any());
    verify(parseResult, times(0)).setUserInput(any());
  }
}
