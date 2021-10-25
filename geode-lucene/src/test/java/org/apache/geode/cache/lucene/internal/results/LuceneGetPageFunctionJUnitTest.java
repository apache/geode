/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.results;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.internal.security.LegacySecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneGetPageFunctionJUnitTest {

  @Test
  public void shouldReturnMapWithKeyAndValue() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    InternalRegionFunctionContext context = mock(InternalRegionFunctionContext.class);
    when(context.getDataSet()).thenReturn(region);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);
    LuceneGetPageFunction function = new LuceneGetPageFunction();
    when(context.getLocalDataSet(any())).thenReturn(region);
    final EntrySnapshot entry = mock(EntrySnapshot.class);
    when(region.getEntry(any())).thenReturn(entry);
    final RegionEntry regionEntry = mock(RegionEntry.class);
    when(entry.getRegionEntry()).thenReturn(regionEntry);
    when(regionEntry.getValue(any())).thenReturn("value");
    when(context.getFilter()).thenReturn((Set) Collections.singleton("key"));
    InternalCache cache = mock(InternalCache.class);
    when(context.getCache()).thenReturn(cache);
    SecurityService securityService = mock(LegacySecurityService.class);
    when(cache.getSecurityService()).thenReturn(securityService);

    function.execute(context);

    PageResults expectedResults = new PageResults();
    expectedResults.add(new PageEntry("key", "value"));
    verify(resultSender).lastResult(eq(expectedResults));
  }

}
