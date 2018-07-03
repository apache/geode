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
package org.apache.geode.management.internal.cli.functions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.test.junit.categories.UnitTest;

public class GetRegionsFunctionJUnitTest {

  TestResultSender testResultSender = new TestResultSender();
  Set<Region<?, ?>> regions = new HashSet<>();
  Set<Region<?, ?>> subregions = new HashSet<>();
  @Mock
  private RegionAttributes regionAttributes;

  @Mock
  private LocalRegion region;
  @Mock
  private GemFireCacheImpl cache;
  @Mock
  private InternalResourceManager internalResourceManager;
  @Mock
  private FunctionContext functionContext;
  @InjectMocks
  private GetRegionsFunction getRegionsFunction;

  @Before
  public void before() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled", "true");
    getRegionsFunction = new GetRegionsFunction();
    MockitoAnnotations.initMocks(this);

    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(this.cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(this.cache.getResourceManager()).thenReturn(this.internalResourceManager);
    when(functionContext.getResultSender()).thenReturn(testResultSender);
    when(functionContext.getCache()).thenReturn(cache);
  }

  @Test
  public void testExecuteWithoutRegions() throws Exception {
    getRegionsFunction.execute(functionContext);
  }

  @Test
  public void testExecuteWithRegions() throws Exception {
    when(cache.rootRegions()).thenReturn(regions);
    when(region.getFullPath()).thenReturn("/MyRegion");

    when(region.getParentRegion()).thenReturn(null);
    when(region.subregions(true)).thenReturn(subregions);
    when(region.subregions(false)).thenReturn(subregions);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(regionAttributes.getScope()).thenReturn(mock(Scope.class));
    regions.add(region);
    getRegionsFunction.execute(functionContext);
  }

  private static class TestResultSender implements ResultSender {

    @Override
    public void lastResult(final Object lastResult) {}

    @Override
    public void sendResult(final Object oneResult) {}

    @Override
    public void sendException(final Throwable t) {
      throw new RuntimeException(t);
    }
  }

}
