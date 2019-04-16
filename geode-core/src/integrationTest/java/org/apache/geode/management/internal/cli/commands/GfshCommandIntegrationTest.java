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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category({GfshTest.class})
public class GfshCommandIntegrationTest {
  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void invalidCommandWhenNotConnected() throws Exception {
    gfsh.executeAndAssertThat("abc").statusIsError().containsOutput("Command 'abc' not found");
  }

  @Test
  public void invalidCommandWhenConnected() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("abc").statusIsError().containsOutput("Command 'abc' not found");
  }

  @Test
  public void commandsAreLoggedAndRedacted() {
    Logger logger = (Logger) LogManager.getLogger(CommandExecutor.class);
    ListAppender listAppender = new ListAppender("ListAppender");
    logger.addAppender(listAppender);
    listAppender.start();

    gfsh.executeAndAssertThat(
        "start locator --properties-file=unknown --J=-Dgemfire.security-password=bob")
        .statusIsError();
    gfsh.executeAndAssertThat("connect --jmx-manager=localhost[999] --password=secret")
        .statusIsError();

    List<LogEvent> logEvents = listAppender.getEvents();
    assertThat(logEvents.size()).as("There should be exactly 2 log events").isEqualTo(2);

    String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
    assertThat(logEvents.get(0).getMessage().getFormattedMessage()).isEqualTo(
        "Executing command: start locator --properties-file=unknown --J=-Dgemfire.security-password=********");
    assertThat(logEvents.get(1).getMessage().getFormattedMessage())
        .isEqualTo("Executing command: connect --jmx-manager=localhost[999] --password=********");

    logger.removeAppender(listAppender);
  }
}
