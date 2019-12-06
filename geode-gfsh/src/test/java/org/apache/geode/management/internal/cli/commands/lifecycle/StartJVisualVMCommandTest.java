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


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GfshParserRule;

public class StartJVisualVMCommandTest {
  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private StartJVisualVMCommand command;

  @Before
  public void setUp() throws Exception {
    command = spy(StartJVisualVMCommand.class);
  }

  @Test
  public void successOutput() throws Exception {
    doReturn("some output").when(command).getProcessOutput(null);
    gfshParser.executeAndAssertThat(command, "start jvisualvm")
        .hasInfoSection().hasOutput().contains("some output");
  }
}
