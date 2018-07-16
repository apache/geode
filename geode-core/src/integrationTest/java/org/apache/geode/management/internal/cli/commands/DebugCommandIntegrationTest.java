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

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GfshCommandRule;


public class DebugCommandIntegrationTest {
  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void debugWithCorrectValues() {
    assertThat(gfsh.getGfsh().getDebug()).isFalse();
    gfsh.executeAndAssertThat("debug --state=ON").statusIsSuccess();
    assertThat(gfsh.getGfsh().getDebug()).isTrue();

    gfsh.executeAndAssertThat("debug --state=off").statusIsSuccess();
    assertThat(gfsh.getGfsh().getDebug()).isFalse();
  }

  @Test
  public void debugWithIncorrectValues() {
    gfsh.executeAndAssertThat("debug --state=true").statusIsError()
        .containsOutput("Invalid state value : true. It should be \"ON\" or \"OFF\"");
    gfsh.executeAndAssertThat("debug --state=0").statusIsError()
        .containsOutput("Invalid state value : 0. It should be \"ON\" or \"OFF\"");
  }
}
