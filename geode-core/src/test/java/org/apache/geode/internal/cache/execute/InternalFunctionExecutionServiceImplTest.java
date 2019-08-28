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
package org.apache.geode.internal.cache.execute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.metrics.FunctionInstrumentor;

public class InternalFunctionExecutionServiceImplTest {

  private InternalFunctionExecutionServiceImpl functionExecutionService;

  @Before
  public void setUp() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache internalCache = mock(InternalCache.class);
    FunctionInstrumentor functionInstrumentor = mock(FunctionInstrumentor.class);

    when(internalDistributedSystem.getCache()).thenReturn(internalCache);
    when(internalCache.getMeterRegistry()).thenReturn(meterRegistry);
    when(functionInstrumentor.instrument(any())).then(returnsFirstArg());

    functionExecutionService =
        spy(new InternalFunctionExecutionServiceImpl(() -> internalDistributedSystem,
            functionInstrumentor));
  }

  @Test
  public void onRegionShouldThrowExceptionWhenRegionIsNull() {
    assertThatThrownBy(() -> functionExecutionService.onRegion(null))
        .isInstanceOf(FunctionException.class)
        .hasMessage("Region instance passed is null");
  }

  @Test
  public void onRegionShouldThrowExceptionWhenThePoolAssociatedWithTheRegionCanNotBeFound() {
    when(functionExecutionService.findPool(any())).thenReturn(null);

    Region region = mock(Region.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPoolName()).thenReturn("testPool");

    assertThatThrownBy(() -> functionExecutionService.onRegion(region))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Could not find a pool named testPool.");
  }

  @Test
  public void onRegionShouldThrowExceptionWhenMultiUserAuthenticationIsSetForNonProxyRegions() {
    Pool pool = mock(Pool.class);
    Region region = mock(Region.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(functionExecutionService.findPool(any())).thenReturn(pool);
    when(pool.getMultiuserAuthentication()).thenReturn(true);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPoolName()).thenReturn("testPool");

    assertThatThrownBy(() -> functionExecutionService.onRegion(region))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void onRegionShouldReturnClientExecutorImplementationForClientRegions() {
    LocalRegion region = mock(LocalRegion.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(region.hasServerProxy()).thenReturn(true);
    when(regionAttributes.getPoolName()).thenReturn(null);

    Execution value = functionExecutionService.onRegion(region);

    assertThat(value)
        .isInstanceOf(ServerRegionFunctionExecutor.class);
  }

  @Test
  public void onRegionShouldReturnPartitionExecutorImplementationForPartitionedRegions() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPoolName()).thenReturn(null);

    Execution value = functionExecutionService.onRegion(region);

    assertThat(value)
        .isInstanceOf(PartitionedRegionFunctionExecutor.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void registerFunction_registersInstrumentingFunction() {
    FunctionInstrumentor functionInstrumentor = mock(FunctionInstrumentor.class);
    Function instrumentingFunction = functionWithId("foo");
    when(functionInstrumentor.instrument(any())).thenReturn(instrumentingFunction);
    functionExecutionService =
        new InternalFunctionExecutionServiceImpl(() -> mock(InternalDistributedSystem.class),
            functionInstrumentor);

    functionExecutionService.registerFunction(functionWithId("foo"));

    Function registeredFunction = functionExecutionService.getFunction("foo");
    assertThat(registeredFunction)
        .isSameAs(instrumentingFunction);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void unregisterFunction_closesInstrumentingFunction() {
    FunctionInstrumentor functionInstrumentor = mock(FunctionInstrumentor.class);
    CloseableFunction instrumentingFunction = functionWithId("foo");
    when(functionInstrumentor.instrument(any())).thenReturn(instrumentingFunction);
    functionExecutionService =
        new InternalFunctionExecutionServiceImpl(() -> mock(InternalDistributedSystem.class),
            functionInstrumentor);
    functionExecutionService.registerFunction(functionWithId("foo"));

    functionExecutionService.unregisterFunction("foo");

    verify(functionInstrumentor).close(same(instrumentingFunction));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void unregisterAllFunctions_closesAllInstrumentingFunction() {
    CloseableFunction instrumentingFunction1 = functionWithId("foo");
    CloseableFunction instrumentingFunction2 = functionWithId("bar");

    FunctionInstrumentor functionInstrumentor = mock(FunctionInstrumentor.class);
    when(functionInstrumentor.instrument(any()))
        .thenReturn(instrumentingFunction1, instrumentingFunction2);

    functionExecutionService =
        new InternalFunctionExecutionServiceImpl(() -> mock(InternalDistributedSystem.class),
            functionInstrumentor);
    functionExecutionService.registerFunction(functionWithId("foo"));
    functionExecutionService.registerFunction(functionWithId("bar"));

    functionExecutionService.unregisterAllFunctions();

    verify(functionInstrumentor).close(same(instrumentingFunction1));
    verify(functionInstrumentor).close(same(instrumentingFunction2));
  }

  private CloseableFunction functionWithId(String id) {
    CloseableFunction function = mock(CloseableFunction.class);
    when(function.getId()).thenReturn(id);
    return function;
  }

  private interface CloseableFunction extends Function, AutoCloseable {

  }
}
