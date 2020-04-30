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

import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class BootstrappingFunctionTest {

  private Cache cache;
  private DistributionManager distributionManager;
  private InternalDistributedMember distributedMember;
  private InternalDistributedSystem distributedSystem;

  private BootstrappingFunction bootstrappingFunction;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(Cache.class);
    distributedMember = mock(InternalDistributedMember.class);
    distributionManager = mock(DistributionManager.class);
    distributedSystem = mock(InternalDistributedSystem.class);

    when(distributedSystem.getDistributedMember()).thenReturn(distributedMember);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);

    bootstrappingFunction = spy(new BootstrappingFunction(
        functionId -> true,
        function -> {
        },
        member -> mock(Execution.class),
        () -> cache,
        () -> cache,
        () -> mock(CreateRegionFunction.class),
        () -> mock(TouchPartitionedRegionEntriesFunction.class),
        () -> mock(TouchReplicatedRegionEntriesFunction.class),
        () -> mock(RegionSizeFunction.class)));
  }

  @Test
  public void isLocatorReturnsTrueForLocatorMember() {
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);

    boolean value = bootstrappingFunction.isLocator(cache);

    assertThat(value).isTrue();
  }

  @Test
  public void isLocatorReturnsFalseForNonLocatorMember() {
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);

    boolean value = bootstrappingFunction.isLocator(cache);

    assertThat(value).isFalse();
  }

  @Test
  public void registerFunctionIsNotCalledOnLocator() {
    FunctionContext functionContext = mock(FunctionContext.class);
    ResultSender<String> resultSender = cast(mock(ResultSender.class));
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.LOCATOR_DM_TYPE);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(functionContext.getResultSender()).thenReturn(resultSender);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);
    doNothing().when(resultSender).lastResult(any());
    doReturn(cache).when(bootstrappingFunction).verifyCacheExists();

    bootstrappingFunction.execute(functionContext);

    verify(bootstrappingFunction, never()).registerFunctions();
  }

  @Test
  public void registerFunctionGetsCalledOnNonLocators() {
    FunctionContext functionContext = mock(FunctionContext.class);
    ResultSender<String> resultSender = cast(mock(ResultSender.class));
    when(distributedMember.getVmKind()).thenReturn(ClusterDistributionManager.NORMAL_DM_TYPE);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(functionContext.getResultSender()).thenReturn(resultSender);
    doNothing().when(distributionManager).addMembershipListener(bootstrappingFunction);
    doNothing().when(resultSender).lastResult(any());
    doReturn(cache).when(bootstrappingFunction).verifyCacheExists();

    bootstrappingFunction.execute(functionContext);

    verify(bootstrappingFunction).registerFunctions();
  }
}
