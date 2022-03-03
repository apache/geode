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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.management.internal.cli.domain.RegionInformation;

/**
 * Characterization unit tests for {@code GetRegionsFunction};
 */
public class GetRegionsFunctionTest {

  @Mock
  private RegionAttributes<Object, Object> regionAttributes;

  @Mock
  private Region<Object, Object> region;

  @Mock
  private FunctionContext<Void> functionContext;

  @Mock
  private ResultSender<RegionInformation[]> resultSender;

  private final GetRegionsFunction getRegionsFunction = new GetRegionsFunction();

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    when(functionContext.<RegionInformation[]>getResultSender()).thenReturn(resultSender);
  }

  @Test
  public void lastResultIsNullWhenThereAreNoRegions() {
    Cache cacheFromFunctionContext = mock(Cache.class);
    when(cacheFromFunctionContext.rootRegions()).thenReturn(Collections.emptySet());
    when(functionContext.getCache()).thenReturn(cacheFromFunctionContext);

    getRegionsFunction.execute(functionContext);

    verify(resultSender).lastResult(isNull());
  }

  @Test
  public void lastResultHasRegionInformationForRegion() {
    String regionNameInCacheFromFunctionContext = "MyRegion";
    Cache cacheFromFunctionContext = cacheWithOneRootRegion(regionNameInCacheFromFunctionContext);
    when(functionContext.getCache()).thenReturn(cacheFromFunctionContext);

    getRegionsFunction.execute(functionContext);

    assertThat(lastResultFrom(resultSender).getValue())
        .extracting(RegionInformation::getPath)
        .containsExactly(regionNameInCacheFromFunctionContext);
  }

  @Test
  public void getsCacheFromFunctionContext() {
    Cache cacheFromFunctionContext = mock(Cache.class);
    when(cacheFromFunctionContext.rootRegions()).thenReturn(Collections.emptySet());
    when(functionContext.getCache()).thenReturn(cacheFromFunctionContext);

    getRegionsFunction.execute(functionContext);

    verify(functionContext).getCache();
  }

  private Cache cacheWithOneRootRegion(String regionName) {
    Cache cache = mock(Cache.class);
    when(regionAttributes.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(regionAttributes.getScope()).thenReturn(mock(Scope.class));
    when(region.getFullPath()).thenReturn(SEPARATOR + regionName);
    when(region.subregions(anyBoolean())).thenReturn(Collections.emptySet());
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(cache.rootRegions()).thenReturn(Collections.singleton(region));
    return cache;
  }

  private ArgumentCaptor<RegionInformation[]> lastResultFrom(
      ResultSender<RegionInformation[]> resultSender) {
    ArgumentCaptor<RegionInformation[]> lastResult =
        ArgumentCaptor.forClass(RegionInformation[].class);
    verify(resultSender).lastResult(lastResult.capture());
    return lastResult;
  }
}
