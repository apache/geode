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

package org.apache.geode.cache.wan.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.cache.wan.internal.parallel.ParallelGatewaySenderTypeFactory;
import org.apache.geode.cache.wan.internal.serial.SerialGatewaySenderTypeFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributesImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderException;

public class GatewaySenderFactoryImplTest {

  @Test
  public void getGatewaySenderTypeFactoryWithIsParallelTrueReturnsAParallelGatewaySenderTypeFactory() {
    final GatewaySenderAttributes attributes = mock(GatewaySenderAttributes.class);
    when(attributes.isParallel()).thenReturn(true);
    assertThat(GatewaySenderFactoryImpl.getGatewaySenderTypeFactory(attributes)).isInstanceOf(
        ParallelGatewaySenderTypeFactory.class);
  }

  @Test
  public void getGatewaySenderTypeFactoryWithIsParallelFalseReturnsASerialGatewaySenderTypeFactory() {
    final GatewaySenderAttributes attributes = mock(GatewaySenderAttributes.class);
    when(attributes.isParallel()).thenReturn(false);
    assertThat(GatewaySenderFactoryImpl.getGatewaySenderTypeFactory(attributes)).isInstanceOf(
        SerialGatewaySenderTypeFactory.class);
  }

  @Test
  public void validateThrowsIfRemoteSystemIdEqualsLocalSystemId() {
    final InternalCache cache = mock(InternalCache.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getDistributedSystemId()).thenReturn(42);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    final GatewaySenderAttributesImpl attributes = mock(GatewaySenderAttributesImpl.class);
    when(attributes.getRemoteDSId()).thenReturn(42);

    assertThatThrownBy(() -> GatewaySenderFactoryImpl.validate(cache, attributes)).isInstanceOf(
        GatewaySenderException.class).hasMessageContaining("remote DS Id equal to this DS Id");
  }

  @Test
  public void validateThrowsIfRemoteSystemIdLessThan0() {
    final InternalCache cache = mock(InternalCache.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getDistributedSystemId()).thenReturn(42);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    final GatewaySenderAttributesImpl attributes = mock(GatewaySenderAttributesImpl.class);
    when(attributes.getRemoteDSId()).thenReturn(-1);

    assertThatThrownBy(() -> GatewaySenderFactoryImpl.validate(cache, attributes)).isInstanceOf(
        GatewaySenderException.class).hasMessageContaining("remote DS Id less than 0");
  }

  @Test
  public void validateThrowsIfDispatcherThreadsLessThan1() {
    final InternalCache cache = mock(InternalCache.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    final GatewaySenderAttributesImpl attributes = mock(GatewaySenderAttributesImpl.class);
    when(attributes.getRemoteDSId()).thenReturn(42);
    when(attributes.getDispatcherThreads()).thenReturn(0);

    assertThatThrownBy(() -> GatewaySenderFactoryImpl.validate(cache, attributes)).isInstanceOf(
        GatewaySenderException.class).hasMessageContaining("dispatcher threads less than 1");
  }

  @Test
  public void validateDoesNotThrow() {
    final InternalCache cache = mock(InternalCache.class);
    final DistributionManager distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    final GatewaySenderAttributesImpl attributes = mock(GatewaySenderAttributesImpl.class);
    when(attributes.getRemoteDSId()).thenReturn(42);
    when(attributes.getDispatcherThreads()).thenReturn(1);
    when(attributes.isBatchConflationEnabled()).thenReturn(false);

    assertThatNoException().isThrownBy(() -> GatewaySenderFactoryImpl.validate(cache, attributes));
  }

}
