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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.HashSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;

public class TouchReplicatedRegionEntriesFunctionTest {

  private FunctionContext context;
  private Cache cache;
  private Region region;
  private ResultSender resultSender;
  private String regionName;
  private HashSet<String> keys;

  private TouchReplicatedRegionEntriesFunction touchReplicatedRegionEntriesFunction;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    cache = mock(Cache.class);
    context = mock(RegionFunctionContext.class);
    keys = new HashSet<>();
    region = mock(Region.class);
    regionName = "regionName";
    resultSender = mock(ResultSender.class);

    when(context.getArguments()).thenReturn(new Object[] {regionName, keys});
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);

    touchReplicatedRegionEntriesFunction = spy(new TouchReplicatedRegionEntriesFunction());
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContext() {
    when(cache.getRegion(regionName)).thenReturn(region);

    touchReplicatedRegionEntriesFunction.execute(context);

    verify(region).getAll(keys);
    verify(resultSender).lastResult(true);
  }

  @Test
  public void executeDoesNotThrowExceptionWithProperlyDefinedContextAndNullRegion() {
    when(cache.getRegion(regionName)).thenReturn(null);

    touchReplicatedRegionEntriesFunction.execute(context);

    verify(region, times(0)).getAll(keys);
    verify(resultSender).lastResult(true);
  }
}
