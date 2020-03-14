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

public class ListDriversCommandTest {

  private ListDriversCommand command;
  private Set<DistributedMember> memberSet;
  private List<CliFunctionResult> resultList = new ArrayList<>();
  private CliFunctionResult result;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    command = spy(new ListDriversCommand());
    memberSet = mock(HashSet.class);
    doReturn(memberSet).when(command).findMembers(any(), any());
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(), any(), any());
  }

  @Test
  public void testListDriverDoesNotThrowException() {
    when(memberSet.size()).thenReturn(1);
    List<String> driverNames = new ArrayList<>();
    driverNames.add("Driver.Class.One");
    driverNames.add("Driver.Class.Two");
    driverNames.add("Driver.Class.Three");
    result = new CliFunctionResult("Server 1", driverNames,
        "{Driver.Class.One, Driver.Class.Two, Driver.Class.Three}");
    resultList.add(result);

    ResultModel resultModel = command.listDrivers(null);

    assertThat(resultModel.toString()).contains("Driver.Class.One").contains("Driver.Class.Two")
        .contains("Driver.Class.Three");

    resultList.clear();
  }

  @Test
  public void testListDriverWithMemberNameDoesNotThrowException() {
    when(memberSet.size()).thenReturn(1);
    List<String> driverNames = new ArrayList<>();
    driverNames.add("Driver.Class.One");
    driverNames.add("Driver.Class.Two");
    driverNames.add("Driver.Class.Three");
    result = new CliFunctionResult("Server 1", driverNames,
        "{Driver.Class.One, Driver.Class.Two, Driver.Class.Three}");
    resultList.add(result);

    ResultModel resultModel = command.listDrivers("Server 1");

    assertThat(resultModel.toString()).contains("Driver.Class.One").contains("Driver.Class.Two")
        .contains("Driver.Class.Three").contains("Server 1");

    resultList.clear();
  }

  @Test
  public void testListDriverReturnsErrorWhenNoMembers() {
    when(memberSet.size()).thenReturn(0);
    ResultModel resultModel = command.listDrivers(null);

    assertThat(resultModel.toString()).contains(NO_MEMBERS_FOUND);
  }

  @Test
  public void testListDriverReturnsWhenFunctionFailsToExecute() {
    when(memberSet.size()).thenReturn(1);
    String errorMessage = "Error message";

    result = new CliFunctionResult("Server 1", CliFunctionResult.StatusState.ERROR,
        errorMessage);
    resultList.add(result);

    ResultModel resultModel = command.listDrivers(null);

    assertThat(resultModel.toString()).contains(errorMessage);
    assertThat(resultModel.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  @Test
  public void testListDriverWithInvalidMemberNameReportsError() {
    String badMemberName = "Bad Member Name";
    when(memberSet.size()).thenReturn(1);
    List<String> driverNames = new ArrayList<>();
    driverNames.add("Driver.Class.One");
    driverNames.add("Driver.Class.Two");
    driverNames.add("Driver.Class.Three");
    result = new CliFunctionResult("Server 1", driverNames,
        "{Driver.Class.One, Driver.Class.Two, Driver.Class.Three}");
    resultList.add(result);

    ResultModel resultModel = command.listDrivers(badMemberName);

    assertThat(resultModel.toString()).contains("No member found with name: " + badMemberName);

    resultList.clear();
  }
}
