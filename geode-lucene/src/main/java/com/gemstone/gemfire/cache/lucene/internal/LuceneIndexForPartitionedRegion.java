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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/* wrapper of IndexWriter */
public class LuceneIndexForPartitionedRegion extends LuceneIndexImpl {

  public LuceneIndexForPartitionedRegion(String indexName, String regionPath, Cache cache) {
    super(indexName, regionPath, cache);
  }

  @Override
  public void initialize() {
    if (!hasInitialized) {
      /* create index region */
      PartitionedRegion dataRegion = (PartitionedRegion) cache.getRegion(regionPath);
      //assert dataRegion != null;
      RegionAttributes ra = dataRegion.getAttributes();
      DataPolicy dp = ra.getDataPolicy();
      final boolean withPersistence = dp.withPersistence();
      final boolean withStorage = ra.getPartitionAttributes().getLocalMaxMemory()>0;
      RegionShortcut regionShortCut;
      if (withPersistence) {
        // TODO: add PartitionedRegionAttributes instead
        regionShortCut = RegionShortcut.PARTITION_PERSISTENT;
      } else {
        regionShortCut = RegionShortcut.PARTITION;
      }

      // final boolean isOffHeap = ra.getOffHeap();

      // TODO: 1) dataRegion should be withStorage
      //       2) Persistence to Persistence
      //       3) Replicate to Replicate, Partition To Partition
      //       4) Offheap to Offheap
      if (!withStorage) {
        throw new IllegalStateException("The data region to create lucene index should be with storage");
      }

      // create PR fileRegion, but not to create its buckets for now
      final String fileRegionName = createFileRegionName();
      PartitionAttributes partitionAttributes = dataRegion.getPartitionAttributes();
      if (!fileRegionExists(fileRegionName)) {
        fileRegion = createFileRegion(regionShortCut, fileRegionName, partitionAttributes);
      }

      // create PR chunkRegion, but not to create its buckets for now
      final String chunkRegionName = createChunkRegionName();
      if (!chunkRegionExists(chunkRegionName)) {
        chunkRegion = createChunkRegion(regionShortCut, fileRegionName, partitionAttributes, chunkRegionName);
      }

      // we will create RegionDirectories on the fly when data comes in
      HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(getFieldNames());
      repositoryManager = new PartitionedRepositoryManager(dataRegion, (PartitionedRegion)fileRegion, (PartitionedRegion)chunkRegion, mapper, analyzer);
      
      // create AEQ, AEQ listener and specify the listener to repositoryManager
      createAEQ(dataRegion);

      addExtension(dataRegion);
      hasInitialized = true;
    }
  }

  private AsyncEventQueueFactoryImpl createAEQFactory(final Region dataRegion) {
    AsyncEventQueueFactoryImpl factory = (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
    factory.setParallel(true); // parallel AEQ for PR
    factory.setMaximumQueueMemory(1000);
    factory.setDispatcherThreads(1);
    factory.setIsMetaQueue(true);
    if(dataRegion.getAttributes().getDataPolicy().withPersistence()) {
      factory.setPersistent(true);
    }
    factory.setDiskSynchronous(dataRegion.getAttributes().isDiskSynchronous());
    return factory;
  }

  AsyncEventQueue createAEQ(Region dataRegion) {
    return createAEQ(createAEQFactory(dataRegion));
  }

  private AsyncEventQueue createAEQ(AsyncEventQueueFactoryImpl factory) {
    LuceneEventListener listener = new LuceneEventListener(repositoryManager);
    String aeqId = LuceneServiceImpl.getUniqueIndexName(getName(), regionPath);
    AsyncEventQueue indexQueue = factory.create(aeqId, listener);
    return indexQueue;
  }

  boolean fileRegionExists(String fileRegionName) {
    return cache.<String, File> getRegion(fileRegionName) != null;
  }

  Region createFileRegion(final RegionShortcut regionShortCut,
                                final String fileRegionName,
                                final PartitionAttributes partitionAttributes) {
    return createRegion(fileRegionName, regionShortCut, this.regionPath, partitionAttributes);
  }

  public String createFileRegionName() {
    return LuceneServiceImpl.getUniqueIndexName(indexName, regionPath)+".files";
  }

  boolean chunkRegionExists(String chunkRegionName) {
    return cache.<ChunkKey, byte[]> getRegion(chunkRegionName) != null;
  }

  Region<ChunkKey, byte[]> createChunkRegion(final RegionShortcut regionShortCut,
                           final String fileRegionName,
                           final PartitionAttributes partitionAttributes, final String chunkRegionName) {
    return createRegion(chunkRegionName, regionShortCut, fileRegionName, partitionAttributes);
  }

  public String createChunkRegionName() {
    return LuceneServiceImpl.getUniqueIndexName(indexName, regionPath) + ".chunks";
  }

  private PartitionAttributesFactory configureLuceneRegionAttributesFactory(PartitionAttributesFactory attributesFactory, PartitionAttributes<?,?> dataRegionAttributes) {
    attributesFactory.setTotalNumBuckets(dataRegionAttributes.getTotalNumBuckets());
    attributesFactory.setRedundantCopies(dataRegionAttributes.getRedundantCopies());
    return attributesFactory;
  }

  protected <K, V> Region<K, V> createRegion(final String regionName, final RegionShortcut regionShortCut,
      final String colocatedWithRegionName, final PartitionAttributes partitionAttributes) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory<String, File>();
    partitionAttributesFactory.setColocatedWith(colocatedWithRegionName);
    configureLuceneRegionAttributesFactory(partitionAttributesFactory, partitionAttributes);

    // Create AttributesFactory based on input RegionShortcut
    RegionAttributes baseAttributes = this.cache.getRegionAttributes(regionShortCut.toString());
    AttributesFactory factory = new AttributesFactory(baseAttributes);
    factory.setPartitionAttributes(partitionAttributesFactory.create());
    RegionAttributes<K, V> attributes = factory.create();

    return createRegion(regionName, attributes);
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
}
