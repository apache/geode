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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import javax.management.remote.JMXServiceURL;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StartJConsoleCommandTest {

  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private StartJConsoleCommand command;
  private Gfsh gfsh;
  ArgumentCaptor<String[]> argumentCaptor;

  @Before
  public void setUp() throws Exception {
    command = spy(StartJConsoleCommand.class);
    gfsh = mock(Gfsh.class);
    doReturn(gfsh).when(command).getGfsh();
    doReturn(null).when(command).getJmxServiceUrl();

    Process process = mock(Process.class);
    doReturn(process).when(command).getProcess(any(String[].class));
    doReturn("some output").when(command).getProcessOutput(process);

    argumentCaptor = ArgumentCaptor.forClass(String[].class);
  }

  @Test
  public void succcessOutput() throws Exception {
    gfshParser.executeAndAssertThat(command, "start jconsole")
        .containsOutput("some output");

    verify(command, times(1)).getProcess(argumentCaptor.capture());

    String[] commandString = argumentCaptor.getValue();
    assertThat(commandString).hasSize(2);
    assertThat(commandString[0]).contains("jconsole");
    assertThat(commandString[1]).isEqualTo("-interval=4");
  }


  @Test
  public void succcessOutputWithVersion() throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append("some error message");
    doReturn(builder).when(command).getErrorStringBuilder(any());

    gfshParser.executeAndAssertThat(command, "start jconsole --version")
        .containsOutput("some error message");

    verify(command, times(1)).getProcess(argumentCaptor.capture());

    String[] commandString = argumentCaptor.getValue();
    assertThat(commandString).hasSize(2);
    assertThat(commandString[0]).contains("jconsole");
    assertThat(commandString[1]).isEqualTo("-version");
  }

  @Test
  public void successWithServiceUrl() throws Exception {
    doReturn(new JMXServiceURL("service:jmx:rmi://localhost")).when(command).getJmxServiceUrl();
    doReturn(true).when(command).isConnectedAndReady();
    gfshParser.executeAndAssertThat(command, "start jconsole")
        .containsOutput("some output");

    verify(command, times(1)).getProcess(argumentCaptor.capture());
    String[] commandString = argumentCaptor.getValue();
    assertThat(commandString).hasSize(3);
    assertThat(commandString[0]).contains("jconsole");
    assertThat(commandString[1]).isEqualTo("-interval=4");
    assertThat(commandString[2]).isEqualTo("service:jmx:rmi://localhost");
  }
}
