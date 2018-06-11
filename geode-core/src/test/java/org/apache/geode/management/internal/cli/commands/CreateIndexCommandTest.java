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

import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class CreateIndexCommandTest {
  @Rule
  public GfshParserRule gfshParser = new GfshParserRule();

  private CreateIndexCommand command;
  private CommandResult result;
  private ResultCollector rc;

  @Before
  public void before() throws Exception {
    command = spy(CreateIndexCommand.class);
    rc = mock(ResultCollector.class);
    when(rc.getResult()).thenReturn(Collections.emptyList());
    doReturn(Collections.emptyList()).when(command).executeAndGetFunctionResult(any(), any(),
        any());
  }

  @Test
  public void missingName() throws Exception {
    gfshParser.executeAndAssertThat(command,
        "create index --expression=abc --region=abc")
        .statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void missingExpression() throws Exception {
    result = gfshParser.executeCommandWithInstance(command, "create index --name=abc --region=abc");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void missingRegion() throws Exception {
    result =
        gfshParser.executeCommandWithInstance(command, "create index --name=abc --expression=abc");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void invalidIndexType() throws Exception {
    result = gfshParser.executeCommandWithInstance(command,
        "create index --name=abc --expression=abc --region=abc --type=abc");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void validIndexType() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    result = gfshParser.executeCommandWithInstance(command,
        "create index --name=abc --expression=abc --region=abc --type=range");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("No Members Found");
  }

  @Test
  public void validIndexType2() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    result = gfshParser.executeCommandWithInstance(command,
        "create index --name=abc --expression=abc --region=abc --type=hash");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("No Members Found");
  }

  @Test
  public void noMemberFound() throws Exception {
    doReturn(Collections.EMPTY_SET).when(command).findMembers(any(), any());
    result = gfshParser.executeCommandWithInstance(command,
        "create index --name=abc --expression=abc --region=abc");
    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains("No Members Found");
  }

  @Test
  public void defaultInexType() throws Exception {
    DistributedMember member = mock(DistributedMember.class);
    doReturn(Collections.singleton(member)).when(command).findMembers(any(), any());

    ArgumentCaptor<RegionConfig.Index> indexTypeCaptor =
        ArgumentCaptor.forClass(RegionConfig.Index.class);
    gfshParser.executeAndAssertThat(command,
        "create index --name=abc --expression=abc --region=abc");

    verify(command).executeAndGetFunctionResult(any(), indexTypeCaptor.capture(),
        eq(Collections.singleton(member)));

    assertThat(indexTypeCaptor.getValue().getType()).isEqualTo("range");
  }

  @Test
  public void getValidRegionName() {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    RegionConfig region = new RegionConfig("regionA.regionB", "REPLICATE");
    when(cacheConfig.findRegionConfiguration("/regionA.regionB")).thenReturn(region);

    assertThat(command.getValidRegionName("regionB", cacheConfig)).isEqualTo("regionB");
    assertThat(command.getValidRegionName("/regionB", cacheConfig)).isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionB b", cacheConfig)).isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionB.entrySet()", cacheConfig))
        .isEqualTo("/regionB");
    assertThat(command.getValidRegionName("/regionA.regionB.entrySet() A", cacheConfig))
        .isEqualTo("/regionA.regionB");
    assertThat(command.getValidRegionName("/regionB.regionA.entrySet() B", cacheConfig))
        .isEqualTo("/regionB");
  }
}
