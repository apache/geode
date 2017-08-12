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

package org.apache.geode.management.internal.cli.help;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.management.internal.cli.commands.GfshHelpCommands;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.annotation.CliCommand;

import java.lang.reflect.Method;

@Category(IntegrationTest.class)
public class HelperIntegrationTest {
  private static Helper helper;

  @BeforeClass
  public static void beforeClass() {
    helper = new Helper();
    // use GfshHelpCommand for testing
    Method[] methods = GfshHelpCommands.class.getMethods();
    for (Method method : methods) {
      CliCommand cliCommand = method.getDeclaredAnnotation(CliCommand.class);
      if (cliCommand != null) {
        helper.addCommand(cliCommand, method);
      }
    }
  }

  @Test
  public void testHelpWithNoInput() {
    String test = helper.getHelp(null, -1);
    String[] helpLines = test.split("\n");
    assertThat(helpLines).hasSize(4);
    assertThat(helpLines[0]).isEqualTo("help (Available)");
    assertThat(helpLines[2]).isEqualTo("hint (Available)");
  }

  @Test
  public void testHelpWithInput() {
    String test = helper.getHelp("help", -1);
    String[] helpLines = test.split("\n");
    assertThat(helpLines).hasSize(12);
    assertThat(helpLines[0]).isEqualTo("NAME");
    assertThat(helpLines[1]).isEqualTo("help");
  }
}
