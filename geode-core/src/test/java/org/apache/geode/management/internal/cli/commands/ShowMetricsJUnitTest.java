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
import static org.mockito.Mockito.spy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class ShowMetricsJUnitTest {

  @Rule
  public GfshParserRule parser = new GfshParserRule();

  @Test
  public void testPortAndRegion() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result =
        parser.executeCommandWithInstance(command, "show metrics --port=0 --region=regionA");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContentAsString())
        .contains("The --region and --port parameters are mutually exclusive");
  }

  @Test
  public void testPortOnly() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result = parser.executeCommandWithInstance(command, "show metrics --port=0");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContentAsString()).contains(
        "If the --port parameter is specified, then the --member parameter must also be specified.");
  }

  @Test
  public void invalidPortNumber() throws Exception {
    ShowMetricsCommand command = spy(ShowMetricsCommand.class);
    CommandResult result = parser.executeCommandWithInstance(command, "show metrics --port=abc");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    // When relying on Spring's converters, any command that does not parse is "Invalid"
    assertThat(result.getContentAsString()).contains("Invalid command");
  }
}
