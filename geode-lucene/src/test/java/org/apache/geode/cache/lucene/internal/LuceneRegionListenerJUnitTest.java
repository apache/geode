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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneRegionListenerJUnitTest {

  @Test
  public void beforeDataRegionCreatedShouldHaveSerializer() {
    String name = "indexName";
    String regionPath = "regionName";
    String[] fields = {"field1", "field2"};
    String aeqId = LuceneServiceImpl.getUniqueIndexName(name, regionPath);
    InternalCache cache = Fakes.cache();
    final Region region = Fakes.region(regionPath, cache);
    RegionAttributes attributes = region.getAttributes();
    DataPolicy policy = attributes.getDataPolicy();
    when(policy.withPartitioning()).thenReturn(true);
    EvictionAttributes evictionAttributes = mock(EvictionAttributes.class);
    when(attributes.getEvictionAttributes()).thenReturn(evictionAttributes);
    CopyOnWriteArraySet set = new CopyOnWriteArraySet();
    set.add(aeqId);
    when(attributes.getAsyncEventQueueIds()).thenReturn(set);
    when(evictionAttributes.getAlgorithm()).thenReturn(EvictionAlgorithm.NONE);
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    Analyzer analyzer = mock(Analyzer.class);
    LuceneSerializer serializer = mock(LuceneSerializer.class);
    InternalRegionArguments internalRegionArgs = mock(InternalRegionArguments.class);
    when(internalRegionArgs.addCacheServiceProfile(any())).thenReturn(internalRegionArgs);

    LuceneRegionListener listener = new LuceneRegionListener(service, name, SEPARATOR + regionPath,
        fields, analyzer, null, serializer);
    listener.beforeCreate(null, regionPath, attributes, internalRegionArgs);
    verify(service).beforeDataRegionCreated(eq(name), eq(SEPARATOR + regionPath), eq(attributes),
        eq(analyzer), any(), eq(aeqId), eq(serializer), any());
  }
}
