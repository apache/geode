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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.cli.UpdateAllConfigurationGroupsMarker;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.security.NotAuthorizedException;

public class CommandExecutorTest {
  private GfshParseResult parseResult;
  private CommandExecutor executor;
  private ResultModel result;
  private SingleGfshCommand testCommand;
  private InternalConfigurationPersistenceService ccService;
  private Region configRegion;


  @Before
  public void before() {
    parseResult = mock(GfshParseResult.class);
    result = new ResultModel();
    executor = spy(CommandExecutor.class);
    testCommand = mock(SingleGfshCommand.class,
        withSettings().extraInterfaces(UpdateAllConfigurationGroupsMarker.class));
    ccService = spy(InternalConfigurationPersistenceService.class);
    configRegion = mock(AbstractRegion.class);


    doReturn(ccService).when(testCommand).getConfigurationPersistenceService();
    doCallRealMethod().when(ccService).updateCacheConfig(any(), any());
    doReturn(true).when(ccService).lockSharedConfiguration();
    doNothing().when(ccService).unlockSharedConfiguration();
    doReturn(configRegion).when(ccService).getConfigurationRegion();

  }


  @Test
  public void executeWhenGivenDummyParseResult() throws Exception {
    Object result = executor.execute(parseResult);
    assertThat(result).isInstanceOf(ResultModel.class);
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
    doThrow(new RuntimeException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
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
    doThrow(new IllegalArgumentException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyIllegalStateExceptionGetsCaught() throws Exception {
    doThrow(new IllegalStateException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyUserErrorExceptionGetsCaught() throws Exception {
    doThrow(new UserErrorException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void anyEntityNotFoundException_statusOK() throws Exception {
    doThrow(new EntityNotFoundException("my message here", true)).when(executor)
        .invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.OK);
    assertThat(thisResult.toString()).contains("Skipping: my message here");
  }

  @Test
  public void anyEntityNotFoundException_statusERROR() throws Exception {
    doThrow(new EntityNotFoundException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
  }

  @Test
  public void invokeCommandWithUpdateAllConfigsInterface_multipleGroupOptionSpecifiedWhenSingleConfiguredGroups_CallsUpdateConfigForGroupTwice() {
    Set<String> configuredGroups = new HashSet<>();
    configuredGroups.add("group1");
    when(parseResult.getParamValueAsString("group")).thenReturn("Group1,Group2");
    doReturn(result).when(executor).callInvokeMethod(any(), any());
    doReturn(configuredGroups).when(ccService).getGroups();

    Object thisResult = executor.invokeCommand(testCommand, parseResult);

    verify(testCommand, times(1)).updateConfigForGroup(eq("Group1"), any(), any());
    verify(testCommand, times(1)).updateConfigForGroup(eq("Group2"), any(), any());
  }

  @Test
  public void invokeCommandWithUpdateAllConfigsInterface_singleGroupOptionSpecifiedWhenMultipleConfiguredGroups_CallsUpdateConfigForGroup() {
    Set<String> configuredGroups = new HashSet<>();
    configuredGroups.add("group1");
    configuredGroups.add("group2");
    when(parseResult.getParamValueAsString("group")).thenReturn("group1");
    doReturn(result).when(executor).callInvokeMethod(any(), any());
    doReturn(configuredGroups).when(ccService).getGroups();

    Object thisResult = executor.invokeCommand(testCommand, parseResult);

    verify(testCommand, times(1)).updateConfigForGroup(eq("group1"), any(), any());
  }

  @Test
  public void invokeCommandWithUpdateAllConfigsInterface_noGroupOptionSpecifiedWhenSingleConfiguredGroups_CallsUpdateConfigForGroup() {
    Set<String> configuredGroups = new HashSet<>();
    configuredGroups.add("group1");
    when(parseResult.getParamValueAsString("group")).thenReturn(null);
    doReturn(result).when(executor).callInvokeMethod(any(), any());
    doReturn(configuredGroups).when(ccService).getGroups();

    Object thisResult = executor.invokeCommand(testCommand, parseResult);

    verify(testCommand, times(1)).updateConfigForGroup(eq("group1"), any(), any());
  }

  @Test
  public void invokeCommandWithOutUpdateAllConfigsInterface_noGroupOptionSpecifiedWhenSingleConfiguredGroups_CallsUpdateConfigForCluster() {
    testCommand = mock(SingleGfshCommand.class);
    doReturn(ccService).when(testCommand).getConfigurationPersistenceService();

    Set<String> configuredGroups = new HashSet<>();
    configuredGroups.add("group1");
    when(parseResult.getParamValueAsString("group")).thenReturn(null);
    doReturn(result).when(executor).callInvokeMethod(any(), any());
    doReturn(configuredGroups).when(ccService).getGroups();

    Object thisResult = executor.invokeCommand(testCommand, parseResult);

    verify(testCommand, times(1)).updateConfigForGroup(eq("cluster"), any(), any());
  }
}
