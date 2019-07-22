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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category(SecurityTest.class)
public class BootstrappingFunctionTest {

  @Test
  public void isLocatorReturnsTrueForLocatorMember() {
    BootstrappingFunction bootstrappingFunction = new BootstrappingFunction();
    GemFireCacheImpl gemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    DistributionManager distributionManager = mock(DistributionManager.class);

    when(gemFireCacheImpl.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(distributedSystem.getDistributedMember()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getVmKind())
        .thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);

    assertThat(bootstrappingFunction.isLocator(gemFireCacheImpl)).isTrue();
  }

  @Test
  public void isLocatorReturnsFalseForNonLocatorMember() {
    BootstrappingFunction bootstrappingFunction = new BootstrappingFunction();
    GemFireCacheImpl gemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    DistributionManager distributionManager = mock(DistributionManager.class);

    when(gemFireCacheImpl.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(distributedSystem.getDistributedMember()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getVmKind())
        .thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);

    assertThat(bootstrappingFunction.isLocator(gemFireCacheImpl)).isFalse();
  }

  @Test
  public void registerFunctionIsNotCalledOnLocator() {
    BootstrappingFunction bootstrappingFunction = spy(new BootstrappingFunction());
    GemFireCacheImpl gemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    DistributionManager distributionManager = mock(DistributionManager.class);

    when(bootstrappingFunction.verifyCacheExists()).thenReturn(gemFireCacheImpl);
    when(gemFireCacheImpl.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(distributedSystem.getDistributedMember()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getVmKind())
        .thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);

    FunctionContext functionContext = mock(FunctionContext.class);
    @SuppressWarnings("unchecked")
    ResultSender<String> resultSender = (ResultSender<String>) mock(ResultSender.class);
    doNothing().when(resultSender).lastResult(any());

    when(functionContext.getResultSender()).thenReturn(resultSender);

    bootstrappingFunction.execute(functionContext);

    verify(bootstrappingFunction, never()).registerFunctions();
  }

  @Test
  public void registerFunctionGetsCalledOnNonLocators() {
    BootstrappingFunction bootstrappingFunction = spy(new BootstrappingFunction());
    GemFireCacheImpl gemFireCacheImpl = mock(GemFireCacheImpl.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    DistributionManager distributionManager = mock(DistributionManager.class);

    when(bootstrappingFunction.verifyCacheExists()).thenReturn(gemFireCacheImpl);
    when(gemFireCacheImpl.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(distributedSystem.getDistributedMember()).thenReturn(internalDistributedMember);
    when(internalDistributedMember.getVmKind())
        .thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);

    FunctionContext functionContext = mock(FunctionContext.class);
    @SuppressWarnings("unchecked")
    ResultSender<String> resultSender = (ResultSender<String>) mock(ResultSender.class);
    doNothing().when(resultSender).lastResult(any());

    when(functionContext.getResultSender()).thenReturn(resultSender);

    bootstrappingFunction.execute(functionContext);

    verify(bootstrappingFunction, times(1)).registerFunctions();
  }
}
