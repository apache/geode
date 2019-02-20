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
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.Mock;

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

public class GetRegionsFunctionTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

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

  private final TestResultSender testResultSender = new TestResultSender();
  private final Set<Region<?, ?>> regions = new HashSet<>();
  private final Set<Region<?, ?>> subregions = new HashSet<>();
  private final GetRegionsFunction getRegionsFunction = new GetRegionsFunction();

  @Before
  public void before() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled", "true");

    initMocks(this);

    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    when(cache.getResourceManager()).thenReturn(internalResourceManager);
    when(functionContext.getResultSender()).thenReturn(testResultSender);
    when(functionContext.getCache()).thenReturn(cache);
  }

  @Test
  public void testExecuteWithoutRegions() {
    getRegionsFunction.execute(functionContext);
  }

  @Test
  public void testExecuteWithRegions() {
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
