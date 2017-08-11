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
package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexForPartitionedRegionTest {

  @Rule
  public ExpectedException expectedExceptions = ExpectedException.none();

  @Test
  public void getIndexNameReturnsCorrectName() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    assertEquals(name, index.getName());
  }

  @Test
  public void getRegionPathReturnsPath() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    assertEquals(regionPath, index.getRegionPath());
  }

  @Test
  public void fileRegionExistsWhenFileRegionExistsShouldReturnTrue() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    PartitionedRegion region = mock(PartitionedRegion.class);
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String fileRegionName = index.createFileRegionName();
    when(cache.getRegion(fileRegionName)).thenReturn(region);

    assertTrue(index.fileRegionExists(fileRegionName));
  }

  @Test
  public void fileRegionExistsWhenFileRegionDoesNotExistShouldReturnFalse() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String fileRegionName = index.createFileRegionName();
    when(cache.getRegion(fileRegionName)).thenReturn(null);

    assertFalse(index.fileRegionExists(fileRegionName));
  }

  @Test
  public void createAEQWithPersistenceCallsCreateOnAEQFactory() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    final Region region = Fakes.region(regionPath, cache);
    RegionAttributes attributes = region.getAttributes();
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    AsyncEventQueueFactoryImpl aeqFactory = mock(AsyncEventQueueFactoryImpl.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(aeqFactory);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.createAEQ(region);

    verify(aeqFactory).setPersistent(eq(true));
    verify(aeqFactory).create(any(), any());
  }

  @Test
  public void createAEQCallsCreateOnAEQFactory() {
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    final Region region = Fakes.region(regionPath, cache);
    AsyncEventQueueFactoryImpl aeqFactory = mock(AsyncEventQueueFactoryImpl.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(aeqFactory);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.createAEQ(region);

    verify(aeqFactory, never()).setPersistent(eq(true));
    verify(aeqFactory).create(any(), any());
  }

  private Region initializeScenario(final boolean withPersistence, final String regionPath,
      final Cache cache) {
    int defaultLocalMemory = 100;
    return initializeScenario(withPersistence, regionPath, cache, defaultLocalMemory);
  }

  private RegionAttributes createRegionAttributes(final boolean withPersistence,
      PartitionAttributes partitionAttributes) {
    AttributesFactory factory = new AttributesFactory();
    if (withPersistence) {
      factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    } else {
      factory.setDataPolicy(DataPolicy.PARTITION);
    }
    factory.setPartitionAttributes(partitionAttributes);
    RegionAttributes ra = factory.create();
    return ra;
  }

  private Region initializeScenario(final boolean withPersistence, final String regionPath,
      final Cache cache, int localMaxMemory) {
    PartitionedRegion region = mock(PartitionedRegion.class);
    PartitionAttributes partitionAttributes = new PartitionAttributesFactory()
        .setLocalMaxMemory(localMaxMemory).setTotalNumBuckets(103).create();
    RegionAttributes regionAttributes =
        spy(createRegionAttributes(withPersistence, partitionAttributes));
    ExtensionPoint extensionPoint = mock(ExtensionPoint.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    when(cache.getRegionAttributes(any())).thenReturn(regionAttributes);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(region.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(region.getExtensionPoint()).thenReturn(extensionPoint);

    return region;
  }

  private PartitionAttributes initializeAttributes(final Cache cache) {
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    RegionAttributes attributes = mock(RegionAttributes.class);
    when(attributes.getCacheListeners()).thenReturn(new CacheListener[0]);
    when(attributes.getRegionTimeToLive()).thenReturn(ExpirationAttributes.DEFAULT);
    when(attributes.getRegionIdleTimeout()).thenReturn(ExpirationAttributes.DEFAULT);
    when(attributes.getEntryTimeToLive()).thenReturn(ExpirationAttributes.DEFAULT);
    when(attributes.getEntryIdleTimeout()).thenReturn(ExpirationAttributes.DEFAULT);
    when(attributes.getMembershipAttributes()).thenReturn(new MembershipAttributes());
    when(cache.getRegionAttributes(RegionShortcut.PARTITION.toString())).thenReturn(attributes);
    when(partitionAttributes.getTotalNumBuckets()).thenReturn(113);
    return partitionAttributes;
  }

  @Test
  public void initializeWithNoLocalMemoryShouldSucceed() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache, 0);
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion spy = setupSpy(region, index, "aeq");
    spy.initialize();
  }

  @Test
  public void initializeWithoutPersistenceShouldCreateAEQ() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion spy = setupSpy(region, index, "aeq");

    verify(spy).createAEQ(eq(region.getAttributes()), eq("aeq"));
  }

  protected LuceneIndexForPartitionedRegion setupSpy(final Region region,
      final LuceneIndexForPartitionedRegion index, final String aeq) {
    index.setSearchableFields(new String[] {"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(any(), any());
    spy.setupRepositoryManager();
    spy.setupAEQ(region.getAttributes(), aeq);
    spy.initialize();
    return spy;
  }

  @Test
  public void initializeShouldCreatePartitionFileRegion() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion spy = setupSpy(region, index, "aeq");

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION), eq(index.createFileRegionName()),
        any(), any(), any());
  }

  @Test
  public void createFileRegionWithPartitionShortcutCreatesRegionUsingCreateVMRegion()
      throws Exception {
    String name = "indexName";
    String regionPath = "regionName";
    GemFireCacheImpl cache = Fakes.cache();
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    PartitionAttributes partitionAttributes = initializeAttributes(cache);
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion indexSpy = spy(index);
    indexSpy.createFileRegion(RegionShortcut.PARTITION, index.createFileRegionName(),
        partitionAttributes, regionAttributes, null);
    String fileRegionName = index.createFileRegionName();
    verify(indexSpy).createRegion(fileRegionName, RegionShortcut.PARTITION, regionPath,
        partitionAttributes, regionAttributes, null);
    verify(cache).createVMRegion(eq(fileRegionName), any(), any());
  }

  @Test
  public void initializeShouldCreatePartitionPersistentFileRegion() {
    boolean withPersistence = true;
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[] {"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ((RegionAttributes) any(), any());
    spy.setupRepositoryManager();
    spy.setupAEQ(any(), any());
    spy.initialize();

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION_PERSISTENT),
        eq(index.createFileRegionName()), any(), any(), any());
  }

  @Test
  public void initializeWhenCalledMultipleTimesShouldNotCreateMultipleFileRegions() {
    boolean withPersistence = true;
    String name = "indexName";
    String regionPath = "regionName";
    InternalCache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[] {"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(any(), any());
    spy.setupRepositoryManager();
    spy.setupAEQ(any(), any());
    spy.initialize();
    spy.initialize();

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION_PERSISTENT),
        eq(index.createFileRegionName()), any(), any(), any());
  }

  @Test
  public void dumpFilesShouldInvokeDumpFunction() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    String[] fields = new String[] {"field1", "field2"};
    InternalCache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    AsyncEventQueue aeq = mock(AsyncEventQueue.class);
    DumpDirectoryFiles function = new DumpDirectoryFiles();
    FunctionService.registerFunction(function);
    LuceneIndexForPartitionedRegion index =
        new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index = spy(index);
    when(index.getFieldNames()).thenReturn(fields);
    doReturn(aeq).when(index).createAEQ(any(), any());
    index.setupRepositoryManager();
    index.setupAEQ(cache.getRegionAttributes(regionPath), aeq.getId());
    index.initialize();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionPath);
    ResultCollector collector = mock(ResultCollector.class);
    when(region.executeFunction(eq(function), any(), any(), anyBoolean())).thenReturn(collector);
    index.dumpFiles("directory");
    verify(region).executeFunction(eq(function), any(), any(), anyBoolean());
  }

}
