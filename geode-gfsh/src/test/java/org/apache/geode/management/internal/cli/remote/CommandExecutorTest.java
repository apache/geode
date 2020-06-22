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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.cli.UpdateAllConfigurationGroupsMarker;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.exceptions.UserErrorException;
import org.apache.geode.security.NotAuthorizedException;

public class CommandExecutorTest {

  private GfshParseResult parseResult;
  private CommandExecutor executor;
  private DistributedLockService dlockService;
  private ResultModel result;
  private SingleGfshCommand testCommand;
  private InternalConfigurationPersistenceService ccService;
  private Region configRegion;

  @Before
  public void setUp() {
    parseResult = mock(GfshParseResult.class);
    result = new ResultModel();
    dlockService = mock(DistributedLockService.class);
    when(dlockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    executor = spy(new CommandExecutor(dlockService));
    testCommand = mock(SingleGfshCommand.class,
        withSettings().extraInterfaces(UpdateAllConfigurationGroupsMarker.class));
    ccService =
        spy(new InternalConfigurationPersistenceService(JAXBService.create(CacheConfig.class)));
    configRegion = mock(AbstractRegion.class);

    doReturn(ccService).when(testCommand).getConfigurationPersistenceService();
    doCallRealMethod().when(ccService).updateCacheConfig(any(), any());
    doReturn(true).when(ccService).lockSharedConfiguration();
    doNothing().when(ccService).unlockSharedConfiguration();
    doReturn(configRegion).when(ccService).getConfigurationRegion();
  }

  @Test
  public void executeWhenGivenDummyParseResult() {
    Object result = executor.execute(parseResult);
    assertThat(result).isInstanceOf(ResultModel.class);
    assertThat(result.toString()).contains("Error while processing command");
  }

  @Test
  public void returnsResultAsExpected() {
    doReturn(result).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(thisResult).isSameAs(result);
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void testNullResult() {
    doReturn(null).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(thisResult.toString()).contains("Command returned null");
  }

  @Test
  public void anyRuntimeExceptionGetsCaught() {
    doThrow(new RuntimeException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void notAuthorizedExceptionGetsThrown() {
    doThrow(new NotAuthorizedException("Not Authorized")).when(executor).invokeCommand(any(),
        any());
    assertThatThrownBy(() -> executor.execute(parseResult))
        .isInstanceOf(NotAuthorizedException.class);
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void anyIllegalArgumentExceptionGetsCaught() {
    doThrow(new IllegalArgumentException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void anyIllegalStateExceptionGetsCaught() {
    doThrow(new IllegalStateException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void anyUserErrorExceptionGetsCaught() {
    doThrow(new UserErrorException("my message here")).when(executor).invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void anyEntityNotFoundException_statusOK() {
    doThrow(new EntityNotFoundException("my message here", true)).when(executor)
        .invokeCommand(any(), any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.OK);
    assertThat(thisResult.toString()).contains("Skipping: my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
  }

  @Test
  public void anyEntityNotFoundException_statusERROR() {
    doThrow(new EntityNotFoundException("my message here")).when(executor).invokeCommand(any(),
        any());
    Object thisResult = executor.execute(parseResult);
    assertThat(((ResultModel) thisResult).getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(thisResult.toString()).contains("my message here");
    verify(executor).lockCMS(any());
    verify(executor).unlockCMS(false);
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

  @Test
  public void nullDlockServiceWillNotLock() throws Exception {
    CommandExecutor nullLockServiceExecutor = new CommandExecutor(null);
    assertThat(nullLockServiceExecutor.lockCMS(new Object())).isEqualTo(false);
  }

  @Test
  public void lockCms() throws Exception {
    assertThat(executor.lockCMS(null)).isEqualTo(false);
    assertThat(executor.lockCMS(new Object())).isEqualTo(false);
    GfshCommand gfshCommand = mock(GfshCommand.class);
    when(gfshCommand.getConfigurationPersistenceService()).thenReturn(null);
    assertThat(executor.lockCMS(gfshCommand)).isEqualTo(false);

    when(gfshCommand.getConfigurationPersistenceService()).thenReturn(ccService);
    when(gfshCommand.affectsClusterConfiguration()).thenReturn(false);
    assertThat(executor.lockCMS(gfshCommand)).isEqualTo(false);

    when(gfshCommand.affectsClusterConfiguration()).thenReturn(true);
    assertThat(executor.lockCMS(gfshCommand)).isEqualTo(true);
    verify(dlockService).lock(any(), eq(-1L), eq(-1L));
  }

  @Test
  public void verifyLockUnlockIsCalledWhenCommandUpdatesCC() throws Exception {
    when(testCommand.getConfigurationPersistenceService()).thenReturn(ccService);
    when(testCommand.affectsClusterConfiguration()).thenReturn(true);
    executor.execute(testCommand, parseResult);
    verify(dlockService).lock(any(), eq(-1L), eq(-1L));
    verify(dlockService).unlock(any());
  }

  @Test
  public void verifyLockUnlockIsNotCalledWhenCommandDoesNotUpdatesCC() throws Exception {
    when(testCommand.getConfigurationPersistenceService()).thenReturn(ccService);
    when(testCommand.affectsClusterConfiguration()).thenReturn(false);
    executor.execute(testCommand, parseResult);
    verify(dlockService, never()).lock(any(), eq(-1L), eq(-1L));
    verify(dlockService, never()).unlock(any());
  }
}
