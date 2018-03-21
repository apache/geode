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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class DestroyGatewaySenderCommandTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private DestroyGatewaySenderCommand command;
  private InternalCache cache;
  private List<CliFunctionResult> functionResults;
  private InternalClusterConfigurationService ccService;
  private CliFunctionResult result1, result2;
  private XmlEntity xmlEntity;

  @Before
  public void before() throws Exception {
    command = spy(DestroyGatewaySenderCommand.class);
    ccService = mock(InternalClusterConfigurationService.class);
    xmlEntity = mock(XmlEntity.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
    doReturn(ccService).when(command).getSharedConfiguration();
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(),
        any(Set.class));
  }

  @Test
  public void mandatoryOptions() throws Exception {
    assertThat(parser.parse("destroy gateway-sender --member=A")).isNull();
  }

  @Test
  public void allFunctionReturnsOK() throws Exception {
    result1 = new CliFunctionResult("member", xmlEntity, "result1");
    result2 = new CliFunctionResult("member", xmlEntity, "result2");
    functionResults.add(result1);
    functionResults.add(result2);

    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    parser.executeAndAssertThat(command, "destroy gateway-sender --id=1").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Status", "result1", "result2");

    verify(ccService).deleteXmlEntity(any(), any());
  }

  @Test
  public void oneFunctionReturnsError() throws Exception {
    result1 = new CliFunctionResult("member", true, "result1");
    result2 = new CliFunctionResult("member", false, "result2");
    functionResults.add(result1);
    functionResults.add(result2);

    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    parser.executeAndAssertThat(command, "destroy gateway-sender --id=1").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Status", "result1", "ERROR: result2");

  }

  @Test
  public void oneFunctionThrowsGeneralException() throws Exception {
    result1 = new CliFunctionResult("member", true, "result1");
    result2 = new CliFunctionResult("member", new Exception("something happened"), null);
    functionResults.add(result1);
    functionResults.add(result2);

    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    parser.executeAndAssertThat(command, "destroy gateway-sender --id=1").statusIsSuccess()
        .tableHasColumnWithValuesContaining("Status", "result1",
            "ERROR: java.lang.Exception: something happened");

  }

  @Test
  public void whenNoCCService() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(null).when(command).getSharedConfiguration();
    result1 = new CliFunctionResult("member", xmlEntity, "result1");
    functionResults.add(result1);
    parser.executeAndAssertThat(command, "destroy gateway-sender --id=1").statusIsSuccess()
        .hasFailToPersistError();
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  @Test
  public void whenCommandOnMember() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getSharedConfiguration();
    result1 = new CliFunctionResult("member", xmlEntity, "result1");
    functionResults.add(result1);
    parser.executeAndAssertThat(command, "destroy gateway-sender --member=xyz --id=1")
        .statusIsSuccess().hasFailToPersistError();
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  @Test
  public void whenNoXml() {
    doReturn(mock(Set.class)).when(command).getMembers(any(), any());
    doReturn(ccService).when(command).getSharedConfiguration();
    result1 = new CliFunctionResult("member", false, "result1");
    functionResults.add(result1);

    // does not delete because command failed, so hasNoFailToPersistError should still be true
    parser.executeAndAssertThat(command, "destroy gateway-sender --id=1").statusIsError()
        .hasNoFailToPersistError();
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }
}
