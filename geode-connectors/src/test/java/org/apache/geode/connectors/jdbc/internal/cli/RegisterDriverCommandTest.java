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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.apache.geode.connectors.jdbc.internal.cli.ListDriversCommand.NO_MEMBERS_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class RegisterDriverCommandTest {
  private RegisterDriverCommand command;
  private Set<DistributedMember> memberSet;
  private List<CliFunctionResult> resultList = new ArrayList<>();
  private CliFunctionResult result;
  private final String DRIVER_CLASS_NAME = "test-jdbc-driver-class-name";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    command = spy(new RegisterDriverCommand());
    memberSet = mock(HashSet.class);
    doReturn(memberSet).when(command).findMembers(any(), any());
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void testRegisterDriverDoesNotThrowException() {
    when(memberSet.size()).thenReturn(1);

    result = new CliFunctionResult("Server1", CliFunctionResult.StatusState.OK,
        DRIVER_CLASS_NAME + " was successfully registered.");
    resultList.add(result);

    ResultModel resultModel = command.registerDriver(DRIVER_CLASS_NAME);

    assertThat(resultModel.toString())
        .contains(DRIVER_CLASS_NAME + " was successfully registered.");
    assertThat(resultModel.getStatus()).isEqualTo(Result.Status.OK);

    resultList.clear();
  }

  @Test
  public void testRegisterDriverReturnsErrorWhenNoMembers() {
    when(memberSet.size()).thenReturn(0);
    ResultModel resultModel = command.registerDriver(DRIVER_CLASS_NAME);

    assertThat(resultModel.toString()).contains(NO_MEMBERS_FOUND);
  }

  @Test
  public void testRegisterDriverReturnsWhenFunctionFailsToExecute() {
    when(memberSet.size()).thenReturn(1);
    String errorMessage = "Error message";

    result = new CliFunctionResult("Server1", CliFunctionResult.StatusState.ERROR,
        errorMessage);
    resultList.add(result);

    ResultModel resultModel = command.registerDriver(DRIVER_CLASS_NAME);

    assertThat(resultModel.toString()).contains(errorMessage);
    assertThat(resultModel.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testRegisterDriverReturnsWhenExceptionIsThrown() {
    String exceptionString = "Test Exception";
    doThrow(new NullPointerException(exceptionString)).when(command).findMembers(any(), any());

    ResultModel resultModel = command.registerDriver(DRIVER_CLASS_NAME);

    assertThat(resultModel.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(resultModel.toString()).contains(
        "Failed to register driver \\\"" + DRIVER_CLASS_NAME + "\\\": " + exceptionString);
  }
}
