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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.cli.shell.Gfsh;

public class UserInputPropertyTest {

  private UserInputProperty userInputProperty;
  private Gfsh gfsh;

  @Before
  public void setUp() throws Exception {
    gfsh = mock(Gfsh.class);
  }

  @Test
  public void propertyWithNoDefaultVaueWillPromptTillAValueIsSupplied_interactive()
      throws Exception {
    userInputProperty = new UserInputProperty("key", "prompt", false);

    when(gfsh.readText(any())).thenReturn("").thenReturn("").thenReturn("value");
    String input = userInputProperty.promptForAcceptableValue(gfsh);

    assertThat(input).isEqualTo("value");
    verify(gfsh, times(0)).readPassword(any());
    verify(gfsh, times(3)).readText(any());
  }

  @Test
  public void propertyWithNoDefaultValue_quietMode() throws Exception {
    when(gfsh.isQuietMode()).thenReturn(true);
    userInputProperty = new UserInputProperty("key", "prompt", false);
    String input = userInputProperty.promptForAcceptableValue(gfsh);
    assertThat(input).isEqualTo("");
    verify(gfsh, times(0)).readPassword(any());
    verify(gfsh, times(0)).readText(any());
  }

  @Test
  public void propertyWithDefaultValue_Interactive() throws Exception {
    userInputProperty = new UserInputProperty("key", "prompt", "value", false);
    String input = userInputProperty.promptForAcceptableValue(gfsh);
    assertThat(input).isEqualTo("value");
    verify(gfsh, times(0)).readPassword(any());
    verify(gfsh).readText(any());
  }

  @Test
  public void propertyWithEmptyDefaultValue_Interactive() throws Exception {
    userInputProperty = new UserInputProperty("key", "prompt", "", false);
    String input = userInputProperty.promptForAcceptableValue(gfsh);
    assertThat(input).isEqualTo("");
    verify(gfsh, times(0)).readPassword(any());
    verify(gfsh).readText(any());
  }

  @Test
  public void propertyWithDefaultValue_Interactive_masked() throws Exception {
    userInputProperty = new UserInputProperty("key", "prompt", "value", true);
    String input = userInputProperty.promptForAcceptableValue(gfsh);
    assertThat(input).isEqualTo("value");
    verify(gfsh).readPassword(any());
    verify(gfsh, times(0)).readText(any());
  }

  @Test
  public void propertyWithDefaultValue_Quiet() throws Exception {
    when(gfsh.isQuietMode()).thenReturn(true);
    userInputProperty = new UserInputProperty("key", "prompt", "value", false);
    String input = userInputProperty.promptForAcceptableValue(gfsh);
    assertThat(input).isEqualTo("value");
    verify(gfsh, times(0)).readPassword(any());
    verify(gfsh, times(0)).readText(any());
  }
}
