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
package org.apache.geode.modules.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class BootstrappingFunctionTest {
  private Cache mockCache;
  private DistributionManager distributionManager;
  private BootstrappingFunction bootstrappingFunction;
  private InternalDistributedMember distributedMember;
  private InternalDistributedSystem distributedSystem;

  @Before
  public void setUp() {
    mockCache = mock(Cache.class);
    bootstrappingFunction = spy(new BootstrappingFunction());
    distributedMember = mock(InternalDistributedMember.class);
    distributionManager = mock(DistributionManager.class);

    distributedSystem = mock(InternalDistributedSystem.class);
    when(distributedSystem.getDistributedMember()).thenReturn(distributedMember);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(mockCache.getDistributedSystem()).thenReturn(distributedSystem);
  }

  @Test
  public void isLocatorReturnsTrueForLocatorMember() {
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);

    assertThat(bootstrappingFunction.isLocator(mockCache)).isTrue();
  }

  @Test
  public void isLocatorReturnsFalseForNonLocatorMember() {
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);

    assertThat(bootstrappingFunction.isLocator(mockCache)).isFalse();
  }

  @Test
  public void registerFunctionIsNotCalledOnLocator() {
    when(bootstrappingFunction.verifyCacheExists(distributedSystem)).thenReturn(mockCache);
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);

    @SuppressWarnings("unchecked")
    ResultSender<String> resultSender = (ResultSender<String>) mock(ResultSender.class);
    FunctionContext functionContext = mock(FunctionContext.class);
    doNothing().when(resultSender).lastResult(any());
    when(functionContext.getResultSender()).thenReturn(resultSender);
    when(functionContext.getCache()).thenReturn(mockCache);

    bootstrappingFunction.execute(functionContext);
    verify(bootstrappingFunction, never()).registerFunctions();
  }

  @Test
  public void registerFunctionGetsCalledOnNonLocators() {
    when(bootstrappingFunction.verifyCacheExists(distributedSystem)).thenReturn(mockCache);
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);

    @SuppressWarnings("unchecked")
    ResultSender<String> resultSender = (ResultSender<String>) mock(ResultSender.class);
    FunctionContext functionContext = mock(FunctionContext.class);
    doNothing().when(resultSender).lastResult(any());
    when(functionContext.getResultSender()).thenReturn(resultSender);
    when(functionContext.getCache()).thenReturn(mockCache);

    bootstrappingFunction.execute(functionContext);
    verify(bootstrappingFunction, times(1)).registerFunctions();
  }
}
