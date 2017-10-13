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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Collections;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class DestroyRegionCommandTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private DestroyRegionCommand command;
  private CommandResult result;

  @Before
  public void before() throws Exception {
    command = spy(DestroyRegionCommand.class);
    doReturn(mock(InternalCache.class)).when(command).getCache();
  }

  @Test
  public void invalidRegion() throws Exception {
    result = parser.executeCommandWithInstance(command, "destroy region");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");

    result = parser.executeCommandWithInstance(command, "destroy region --name=");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");

    result = parser.executeCommandWithInstance(command, "destroy region --name=/");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");
  }

  @Test
  public void whenNoRegionIsFoundOnAnyMembers() throws Exception {
    doReturn(Collections.emptySet()).when(command).findMembersForRegion(any(), any());
    result = parser.executeCommandWithInstance(command, "destroy region --name=test");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);

    result = parser.executeCommandWithInstance(command, "destroy region --name=test --if-exists");
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
  }
}
