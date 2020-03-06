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

import static org.apache.geode.management.internal.i18n.CliStrings.RESUME_ASYNCEVENTQUEUE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ResumeAsyncEventQueueDispatcherCommandTest {

  public static final String COMMAND = RESUME_ASYNCEVENTQUEUE;
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ResumeAsyncEventQueueDispatcherCommand command;

  @Before
  public void before() {
    command = spy(ResumeAsyncEventQueueDispatcherCommand.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void resumeAsyncEventQueueSuccessful() {
    List<CliFunctionResult> functionResults = new ArrayList<>();
    functionResults
        .add(new CliFunctionResult("member1", CliFunctionResult.StatusState.OK, "SUCCESS"));
    functionResults
        .add(new CliFunctionResult("member2", CliFunctionResult.StatusState.ERROR, "FAILURE"));

    doReturn(functionResults).when(command).executeAndGetFunctionResult(isA(Function.class),
        isA(Object.class), isA(Set.class));
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --id=queueId");

    verify(command).executeAndGetFunctionResult(isA(Function.class),
        isA(String.class), isA(Set.class));

    verify(command).constructResultModel(functionResults);
  }
}
