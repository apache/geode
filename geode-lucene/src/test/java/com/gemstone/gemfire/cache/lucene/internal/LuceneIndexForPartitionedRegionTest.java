/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.extension.ExtensionPoint;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexForPartitionedRegionTest {

  @Rule
  public ExpectedException expectedExceptions = ExpectedException.none();

  @Test
  public void getIndexNameReturnsCorrectName() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = null;
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    assertEquals(name, index.getName());
  }

  @Test
  public void getRegionPathReturnsPath() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = null;
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    assertEquals(regionPath, index.getRegionPath());
  }

  @Test
  public void fileRegionExistsWhenFileRegionExistsShouldReturnTrue() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    PartitionedRegion region = mock(PartitionedRegion.class);
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String fileRegionName = index.createFileRegionName();
    when(cache.getRegion(fileRegionName)).thenReturn(region);

    assertTrue(index.fileRegionExists(fileRegionName));
  }

  @Test
  public void fileRegionExistsWhenFileRegionDoesNotExistShouldReturnFalse() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String fileRegionName = index.createFileRegionName();
    when(cache.getRegion(fileRegionName)).thenReturn(null);

    assertFalse(index.fileRegionExists(fileRegionName));
  }

  @Test
  public void chunkRegionExistsWhenChunkRegionExistsShouldReturnTrue() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    PartitionedRegion region = mock(PartitionedRegion.class);
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String chunkRegionName = index.createChunkRegionName();
    when(cache.getRegion(chunkRegionName)).thenReturn(region);

    assertTrue(index.chunkRegionExists(chunkRegionName));
  }

  @Test
  public void chunkRegionExistsWhenChunkRegionDoesNotExistShouldReturnFalse() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    String chunkRegionName = index.createChunkRegionName();
    when(cache.getRegion(chunkRegionName)).thenReturn(null);

    assertFalse(index.chunkRegionExists(chunkRegionName));
  }

  @Test
  public void createAEQWithPersistenceCallsCreateOnAEQFactory() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    final Region region =Fakes.region(regionPath, cache);
    RegionAttributes attributes  = region.getAttributes();
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    AsyncEventQueueFactoryImpl aeqFactory = mock(AsyncEventQueueFactoryImpl.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(aeqFactory);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.createAEQ(region);

    verify(aeqFactory).setPersistent(eq(true));
    verify(aeqFactory).create(any(), any());
  }

  @Test
  public void createAEQCallsCreateOnAEQFactory() {
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    final Region region =Fakes.region(regionPath, cache);
    AsyncEventQueueFactoryImpl aeqFactory = mock(AsyncEventQueueFactoryImpl.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(aeqFactory);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.createAEQ(region);

    verify(aeqFactory, never()).setPersistent(eq(true));
    verify(aeqFactory).create(any(), any());
  }

  private Region initializeScenario(final boolean withPersistence, final String regionPath, final Cache cache) {
    int defaultLocalMemory = 100;
    return initializeScenario(withPersistence, regionPath, cache, defaultLocalMemory);
  }

  private Region initializeScenario(final boolean withPersistence, final String regionPath, final Cache cache, int localMaxMemory) {
    PartitionedRegion region = mock(PartitionedRegion.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    PartitionAttributes partitionAttributes = mock(PartitionAttributes.class);
    DataPolicy dataPolicy = mock(DataPolicy.class);
    ExtensionPoint extensionPoint = mock(ExtensionPoint.class);
    when(cache.getRegion(regionPath)).thenReturn(region);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getPartitionAttributes()).thenReturn(partitionAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(dataPolicy);
    when(partitionAttributes.getLocalMaxMemory()).thenReturn(localMaxMemory);
    when(partitionAttributes.getTotalNumBuckets()).thenReturn(113);
    when(dataPolicy.withPersistence()).thenReturn(withPersistence);
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
  public void initializeWithNoLocalMemoryThrowsException() {
    expectedExceptions.expect(IllegalStateException.class);
    expectedExceptions.expectMessage("The data region to create lucene index should be with storage");
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache, 0);
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.initialize();
  }

  @Test
  public void initializeWithoutPersistenceShouldCreateAEQ() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(eq(region));
    spy.initialize();

    verify(spy).createAEQ(eq(region));
  }

  @Test
  public void initializeShouldCreatePartitionChunkRegion() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(eq(region));
    spy.initialize();

    verify(spy).createChunkRegion(eq(RegionShortcut.PARTITION), eq(index.createFileRegionName()), any(), eq(index.createChunkRegionName()));
  }

  @Test
  public void initializeShouldCreatePartitionFileRegion() {
    boolean withPersistence = false;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    Region region = initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(eq(region));
    spy.initialize();

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION), eq(index.createFileRegionName()), any());
  }

  @Test
  public void createFileRegionWithPartitionShortcutCreatesRegionUsingCreateVMRegion() throws Exception {
    String name = "indexName";
    String regionPath = "regionName";
    GemFireCacheImpl cache = Fakes.cache();
    PartitionAttributes partitionAttributes = initializeAttributes(cache);
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion indexSpy = spy(index);
    indexSpy.createFileRegion(RegionShortcut.PARTITION, index.createFileRegionName(), partitionAttributes);
    String fileRegionName = index.createFileRegionName();
    verify(indexSpy).createRegion(fileRegionName, RegionShortcut.PARTITION, regionPath, partitionAttributes);
    verify(cache).createVMRegion(eq(fileRegionName), any(), any());
  }

  @Test
  public void createChunkRegionWithPartitionShortcutCreatesRegionUsingCreateVMRegion() throws Exception {
    String name = "indexName";
    String regionPath = "regionName";
    GemFireCacheImpl cache = Fakes.cache();
    PartitionAttributes partitionAttributes = initializeAttributes(cache);
    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    LuceneIndexForPartitionedRegion indexSpy = spy(index);
    String chunkRegionName = index.createChunkRegionName();
    String fileRegionName = index.createFileRegionName();
    indexSpy.createChunkRegion(RegionShortcut.PARTITION, fileRegionName, partitionAttributes, chunkRegionName);
    verify(indexSpy).createRegion(chunkRegionName, RegionShortcut.PARTITION, fileRegionName, partitionAttributes);
    verify(cache).createVMRegion(eq(chunkRegionName), any(), any());
  }

  @Test
  public void initializeShouldCreatePartitionPersistentChunkRegion() {
    boolean withPersistence = true;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(any());
    spy.initialize();

    verify(spy).createChunkRegion(eq(RegionShortcut.PARTITION_PERSISTENT), eq(index.createFileRegionName()), any(), eq(index.createChunkRegionName()));
  }

  @Test
  public void initializeShouldCreatePartitionPersistentFileRegion() {
    boolean withPersistence = true;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(any());
    spy.initialize();

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION_PERSISTENT), eq(index.createFileRegionName()), any());
  }

  @Test
  public void initializeWhenCalledMultipleTimesShouldNotCreateMultipleFileRegions() {
    boolean withPersistence = true;
    String name = "indexName";
    String regionPath = "regionName";
    Cache cache = Fakes.cache();
    initializeScenario(withPersistence, regionPath, cache);

    LuceneIndexForPartitionedRegion index = new LuceneIndexForPartitionedRegion(name, regionPath, cache);
    index.setSearchableFields(new String[]{"field"});
    LuceneIndexForPartitionedRegion spy = spy(index);
    doReturn(null).when(spy).createFileRegion(any(), any(), any());
    doReturn(null).when(spy).createChunkRegion(any(), any(), any(), any());
    doReturn(null).when(spy).createAEQ(any());
    spy.initialize();
    spy.initialize();

    verify(spy).createFileRegion(eq(RegionShortcut.PARTITION_PERSISTENT), eq(index.createFileRegionName()), any());
  }

}
