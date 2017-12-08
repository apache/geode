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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class CreateGatewayReceiverCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateGatewayReceiverCommand command;

  @Before
  public void before() {
    command = spy(CreateGatewayReceiverCommand.class);
  }

  @Test
  public void testDefaultValues() {
    GfshParseResult parseResult = gfsh.parse("create gateway-receiver");

    assertThat(parseResult.getParamValue("start-port")).isNull();
    assertThat(parseResult.getParamValue("end-port")).isNull();
    assertThat(parseResult.getParamValue("socket-buffer-size")).isNull();
  }


  @Test
  public void endMustBeLargerThanStart() {
    gfsh.executeAndAssertThat(command, "create gateway-receiver --end-port=1").statusIsError()
        .containsOutput("start-port must be smaller than end-port");

    gfsh.executeAndAssertThat(command, "create gateway-receiver --start-port=60000").statusIsError()
        .containsOutput("start-port must be smaller than end-port");

    gfsh.executeAndAssertThat(command, "create gateway-receiver --end-port=1 --start-port=2")
        .statusIsError().containsOutput("start-port must be smaller than end-port");
  }
}
