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
package org.apache.geode.cache.query.internal.index;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.Statistics;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;

public class CompactRangeIndexTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  private CompactRangeIndex index;
  private LocalRegion region = mock(LocalRegion.class);
  private GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
  private InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
  private Statistics stats = mock(Statistics.class);
  private InternalIndexStatistics indstats = mock(InternalIndexStatistics.class);

  private RegionEntry entry = mock(RegionEntry.class);
  private IndexManager img = mock(IndexManager.class);
  private CachePerfStats cacheperfstat = mock(CachePerfStats.class);

  private DefaultQueryService queryservice = mock(DefaultQueryService.class);

  @Before
  public void setup() {
    when(region.getCache()).thenReturn(cache);
    when(region.getAttributes()).thenReturn(mock(RegionAttributes.class));
    when(cache.getDistributedSystem()).thenReturn(ids);
    when(ids.createAtomicStatistics(any(), eq("Index1"))).thenReturn(stats);
    when(cache.getRegion(any())).thenReturn(region);
    when(img.putCanonicalizedIteratorNameIfAbsent(any())).thenReturn("index_iter");
    when(cache.getQueryService()).thenReturn(mock(InternalQueryService.class));
    when(cache.getQueryService().getMethodInvocationAuthorizer())
        .thenReturn(mock(MethodInvocationAuthorizer.class));

    IndexCreationHelper helper = new FunctionalIndexCreationHelper("/exampleRegion", "status",
        "*", null, cache, null, img);

    index = new CompactRangeIndex(cache, "Index1", region, "/exampleRegion",
        "status", "*", null, null,
        null, indstats);

    index.instantiateEvaluator(helper, mock(ObjectType.class));
  }

  @Test
  public void testAddMapping() throws Exception {
    when(region.getCache()).thenReturn(cache);
    when(cache.getCachePerfStats()).thenReturn(cacheperfstat);
    when(cache.getQueryService()).thenReturn(queryservice);
    when(queryservice.getMethodInvocationAuthorizer()).thenReturn(null);

    index.addMapping(entry);
    verify(indstats).incNumUpdates();

  }

  @Test
  public void testRemoveMapping() throws Exception {
    when(region.getCache()).thenReturn(cache);
    when(cache.getCachePerfStats()).thenReturn(cacheperfstat);
    when(cache.getQueryService()).thenReturn(queryservice);
    when(queryservice.getMethodInvocationAuthorizer()).thenReturn(null);

    index.removeMapping(entry, 3);

  }

  @Test
  public void testRemoveUpdateRemoveMapping() throws Exception {
    when(region.getCache()).thenReturn(cache);
    when(cache.getCachePerfStats()).thenReturn(cacheperfstat);
    when(cache.getQueryService()).thenReturn(queryservice);
    when(queryservice.getMethodInvocationAuthorizer()).thenReturn(null);

    index.removeMapping(entry, 1);
    index.addMapping(entry);
    verify(indstats).incNumUpdates();
    index.removeMapping(entry, 3);

  }
}
