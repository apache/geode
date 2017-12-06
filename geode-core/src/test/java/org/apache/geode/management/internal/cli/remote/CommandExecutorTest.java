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

package org.apache.geode.management.internal.cli.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CommandExecutorTest {
  ParseResult parseResult;
  CommandExecutor executor;
  Result result;

  @Before
  public void before() {
    parseResult = mock(ParseResult.class);
    result = mock(Result.class);
    executor = spy(CommandExecutor.class);
  }


  @Test
  public void executeWhenGivenDummyParseResult() throws Exception {
    Object result = executor.execute(parseResult);
    assertThat(result).isInstanceOf(CommandResult.class);
    assertThat(result.toString()).contains("Error while processing command");
  }

  @Test
  public void returnsResultAsExpected() throws Exception {
    doReturn(result).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(thisResult).isSameAs(result);
  }

  @Test
  public void testNullResult() throws Exception {
    doReturn(null).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(thisResult.toString()).contains("Command returned null");
  }

  @Test
  public void anyRuntimeExceptionGetsCaught() throws Exception {
    ;
    doThrow(new RuntimeException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void notAuthorizedExceptionGetsThrown() throws Exception {
    doThrow(new NotAuthorizedException("Not Authorized")).when(executor).invokeCommand(any(),
        any());
    assertThatThrownBy(() -> executor.execute(parseResult))
        .isInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void anyIllegalArgumentExceptionGetsCaught() throws Exception {
    ;
    doThrow(new IllegalArgumentException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyIllegalStateExceptionGetsCaught() throws Exception {
    ;
    doThrow(new IllegalStateException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyUserErrorExceptionGetsCaught() throws Exception {
    ;
    doThrow(new UserErrorException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyEntityNotFoundException_statusOK() throws Exception {
    ;
    doThrow(new EntityNotFoundException("my message here", true)).when(executor)
        .invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.OK);
    assertThat(thisResult.toString()).contains("Skipping: my message here");
  }

  @Test
  public void anyEntityNotFoundException_statusERROR() throws Exception {
    ;
    doThrow(new EntityNotFoundException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((CommandResult) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }
}
