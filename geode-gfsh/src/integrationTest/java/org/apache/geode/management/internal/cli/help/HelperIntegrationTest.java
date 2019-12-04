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

import static org.apache.geode.management.internal.i18n.CliStrings.HINT__MSG__TOPICS_AVAILABLE;
import static org.apache.geode.management.internal.i18n.CliStrings.HINT__MSG__UNKNOWN_TOPIC;
import static org.apache.geode.management.internal.i18n.CliStrings.TOPIC_CLIENT__DESC;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.management.internal.cli.commands.GfshHelpCommand;
import org.apache.geode.management.internal.cli.commands.GfshHintCommand;
import org.apache.geode.management.internal.i18n.CliStrings;

public class HelperIntegrationTest {
  private static Helper helper;

  @BeforeClass
  public static void beforeClass() {
    helper = new Helper();
  }

  private void getHelpCommand() {
    Method[] methods = GfshHelpCommand.class.getMethods();
    for (Method method : methods) {
      CliCommand cliCommand = method.getDeclaredAnnotation(CliCommand.class);
      if (cliCommand != null) {
        helper.addCommand(cliCommand, method);
      }
    }
  }

  private void getHintCommand() {
    Method[] methods = GfshHintCommand.class.getMethods();
    for (Method method : methods) {
      CliCommand cliCommand = method.getDeclaredAnnotation(CliCommand.class);
      if (cliCommand != null) {
        helper.addCommand(cliCommand, method);
      }
    }
  }

  @Test
  public void testHelpWithNoInput() {
    getHelpCommand();
    String testNoInput = helper.getHelp(null, -1);
    String[] helpLines = testNoInput.split("\n");
    assertThat(helpLines).hasSize(2);
    assertThat(helpLines[0].trim()).isEqualTo("help (Available)");
    assertThat(helpLines[1].trim()).isEqualTo(CliStrings.HELP__HELP);
  }

  @Test
  public void testHelpWithInput() {
    getHelpCommand();
    String testInput = helper.getHelp("help", -1);
    String[] helpLines = testInput.split("\n");
    assertThat(helpLines).hasSize(12);
    assertThat(helpLines[0].trim()).isEqualTo("NAME");
    assertThat(helpLines[1].trim()).isEqualTo("help");
  }

  @Test
  public void testHelpWithInvalidInput() {
    getHelpCommand();
    String testInvalidInput = helper.getHelp("InvalidTopic", -1);
    assertThat(testInvalidInput).isEqualTo("No help exists for this command.");
  }

  @Test
  public void testHintWithNoInput() {
    getHintCommand();
    String testNoInput = helper.getHint(null);
    String[] hintLines = testNoInput.split("\n");
    assertThat(hintLines).hasSize(21);
    assertThat(hintLines[0].trim()).isEqualTo(HINT__MSG__TOPICS_AVAILABLE);
  }

  @Test
  public void testHintWithInput() {
    getHintCommand();
    String testInput = helper.getHint("Client");
    assertThat(testInput).contains(TOPIC_CLIENT__DESC);
  }

  @Test
  public void testHintWithInvalidInput() {
    getHintCommand();
    String testInvalidInput = helper.getHint("InvalidTopic");
    assertThat(testInvalidInput)
        .startsWith(CliStrings.format(HINT__MSG__UNKNOWN_TOPIC, "InvalidTopic"));
  }
}
