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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.help.Helper;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(GfshTest.class)
public class HintCommandIntegrationTest {
  private static Helper hintHelper;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() {
    hintHelper = new Helper();
  }

  @Test
  public void hintCommandShouldSucceedWhenTopicDoesNotExist() {
    gfsh.executeAndAssertThat("hint invalidTopic").statusIsSuccess()
        .containsOutput("Unknown topic", "Use hint to view the list of available topics.");
  }

  @Test
  public void hintCommandWithNoParametersShouldReturnAllTopics() {
    gfsh.executeAndAssertThat("hint").statusIsSuccess()
        .containsOutput(hintHelper.getTopicNames().toArray(new String[0]))
        .doesNotContainOutput("Unknown topic");
  }

  @Test
  public void hintCommandShouldReturnHintForAllKnownTopics() {
    hintHelper.getTopicNames().forEach(
        topic -> gfsh.executeAndAssertThat("hint " + topic).statusIsSuccess()
            .doesNotContainOutput("Unknown topic"));
  }

  @Test
  public void hintCommandShouldIgnoreCase() {
    hintHelper.getTopicNames().forEach(
        topic -> {
          gfsh.executeAndAssertThat("hint " + topic.toLowerCase()).statusIsSuccess()
              .doesNotContainOutput("Unknown topic");
          gfsh.executeAndAssertThat("hint " + topic.toUpperCase()).statusIsSuccess()
              .doesNotContainOutput("Unknown topic");
        });
  }
}
