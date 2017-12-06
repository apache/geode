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

import static io.codearte.catchexception.shade.mockito.Matchers.any;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class CreateAsyncEventQueueCommandTest {

  public static final String COMMAND = "create async-event-queue ";
  public static final String MINIUM_COMMAND = COMMAND + "--id=id --listener=xyz";
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateAsyncEventQueueCommand command;
  private ClusterConfigurationService service;

  @Before
  public void before() throws Exception {
    command = spy(CreateAsyncEventQueueCommand.class);
    service = mock(ClusterConfigurationService.class);
    doReturn(service).when(command).getSharedConfiguration();
  }

  @Test
  public void mandatoryId() throws Exception {
    gfsh.executeAndAssertThat(command, COMMAND + "--listener=xyz").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void mandatoryListener() throws Exception {
    gfsh.executeAndAssertThat(command, COMMAND + "--id=id").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void cannotCreateAEQOnOneMember() throws Exception {
    // AEQ can not be created on one member since it needs to update CC.
    // This test is to make sure we don't add this option
    gfsh.executeAndAssertThat(command, COMMAND + "--id=id --listener=xyz --member=xyz")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void defaultValues() throws Exception {
    GfshParseResult result = gfsh.parse(MINIUM_COMMAND);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL)).isEqualTo(5);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE)).isEqualTo(100);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY)).isEqualTo(100);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS)).isEqualTo(1);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS)).isEqualTo(true);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__PERSISTENT)).isEqualTo(false);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION))
        .isEqualTo(false);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__PARALLEL)).isEqualTo(false);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY))
        .isEqualTo(false);
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY)).isEqualTo("KEY");

    result = gfsh.parse(COMMAND + "--id=id --listener=xyz --forward-expiration-destroy");
    assertThat(result.getParamValue(CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY))
        .isEqualTo(true);
  }

  @Test
  public void noMemberFound() throws Exception {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    gfsh.executeAndAssertThat(command, MINIUM_COMMAND).statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void buildResult_all_success() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    XmlEntity xmlEntity = mock(XmlEntity.class);
    functionResults.add(new CliFunctionResult("member1", xmlEntity, "SUCCESS"));
    functionResults.add(new CliFunctionResult("member2", xmlEntity, "SUCCESS"));

    // this is only to make the code pass that member check
    doReturn(Collections.emptySet()).when(command).getMembers(any(), any());
    doReturn(functionResults).when(command).executeAndGetFunctionResult(isA(Function.class),
        isA(Object.class), isA(Set.class));

    gfsh.executeAndAssertThat(command, MINIUM_COMMAND).statusIsSuccess().persisted()
        .tableHasRowCount("Member", 2)
        .tableHasRowWithValues("Member", "Result", "member1", "SUCCESS")
        .tableHasRowWithValues("Member", "Result", "member2", "SUCCESS");

    // addXmlEntity should only be called once
    verify(service).addXmlEntity(xmlEntity, null);
  }


  @Test
  public void buildResult_all_failure() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    XmlEntity xmlEntity = mock(XmlEntity.class);
    functionResults.add(new CliFunctionResult("member1", false, "failed"));
    functionResults
        .add(new CliFunctionResult("member2", new RuntimeException("exception happened"), null));

    // this is only to make the code pass that member check
    doReturn(Collections.emptySet()).when(command).getMembers(any(), any());
    doReturn(functionResults).when(command).executeAndGetFunctionResult(isA(Function.class),
        isA(Object.class), isA(Set.class));

    gfsh.executeAndAssertThat(command, MINIUM_COMMAND).statusIsSuccess().persisted() // need to make
                                                                                     // sure
                                                                                     // failToPersist
                                                                                     // flag is not
                                                                                     // set, so that
                                                                                     // we won't
                                                                                     // print out
                                                                                     // warning
                                                                                     // messages.
        .tableHasRowCount("Member", 2)
        .tableHasRowWithValues("Member", "Result", "member1", "ERROR: failed")
        .tableHasRowWithValues("Member", "Result", "member2",
            "ERROR: java.lang.RuntimeException: exception happened");

    // addXmlEntity should not be called
    verify(service, times(0)).addXmlEntity(xmlEntity, null);
  }

  @Test
  public void buildResult_one_failure_one_success() throws Exception {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    XmlEntity xmlEntity = mock(XmlEntity.class);
    functionResults.add(new CliFunctionResult("member1", xmlEntity, "SUCCESS"));
    functionResults
        .add(new CliFunctionResult("member2", new RuntimeException("exception happened"), null));

    // this is only to make the code pass that member check
    doReturn(Collections.emptySet()).when(command).getMembers(any(), any());
    doReturn(functionResults).when(command).executeAndGetFunctionResult(isA(Function.class),
        isA(Object.class), isA(Set.class));

    gfsh.executeAndAssertThat(command, MINIUM_COMMAND).statusIsSuccess().persisted()
        .tableHasRowCount("Member", 2)
        .tableHasRowWithValues("Member", "Result", "member1", "SUCCESS").tableHasRowWithValues(
            "Member", "Result", "member2", "ERROR: java.lang.RuntimeException: exception happened");

    // addXmlEntity should be called once
    verify(service).addXmlEntity(xmlEntity, null);
  }

  @Test
  public void command_succeeded_but_no_cluster_config_service() throws Exception {
    doReturn(null).when(command).getSharedConfiguration();
    doReturn(Collections.emptySet()).when(command).getMembers(any(), any());

    List<CliFunctionResult> functionResults = new ArrayList<>();
    XmlEntity xmlEntity = mock(XmlEntity.class);
    functionResults.add(new CliFunctionResult("member1", xmlEntity, "SUCCESS"));
    doReturn(functionResults).when(command).executeAndGetFunctionResult(isA(Function.class),
        isA(Object.class), isA(Set.class));

    gfsh.executeAndAssertThat(command, MINIUM_COMMAND).statusIsSuccess().failToPersist();

    // addXmlEntity should not be called
    verify(service, times(0)).addXmlEntity(xmlEntity, null);
  }
}
