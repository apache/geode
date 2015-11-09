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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/* wrapper of IndexWriter */
public class LuceneIndexForPartitionedRegion extends LuceneIndexImpl {

  private final Cache cache;

  public LuceneIndexForPartitionedRegion(String indexName, String regionPath, Cache cache) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.cache = cache;
  }
  
  @Override
  public void initialize() {
    if (!hasInitialized) {
      /* create index region */
      PartitionedRegion dataRegion = (PartitionedRegion)cache.getRegion(regionPath);
      assert dataRegion != null;
      RegionAttributes ra = dataRegion.getAttributes();
      DataPolicy dp = ra.getDataPolicy();
      final boolean isPartitionedRegion = (ra.getPartitionAttributes() == null) ? false : true;
      final boolean withPersistence = dp.withPersistence();
      final boolean withStorage = isPartitionedRegion?ra.getPartitionAttributes().getLocalMaxMemory()>0:dp.withStorage();
      RegionShortcut regionShortCut;
      if (isPartitionedRegion) {
        if (withPersistence) {
          // TODO: add PartitionedRegionAttributes instead
          regionShortCut = RegionShortcut.PARTITION_PERSISTENT;
        } else {
          regionShortCut = RegionShortcut.PARTITION;
        }
      } else {
        if (withPersistence) {
          regionShortCut = RegionShortcut.REPLICATE_PERSISTENT;
        } else {
          regionShortCut = RegionShortcut.REPLICATE;
        }
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
      final String fileRegionName = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath)+".files";
      fileRegion = cache.<String, File> getRegion(fileRegionName);
      PartitionAttributes partitionAttributes = dataRegion.getPartitionAttributes();
      if (null == fileRegion) {
        fileRegion = cache.<String, File> createRegionFactory(regionShortCut)
            .setPartitionAttributes(new PartitionAttributesFactory<String, File>().setColocatedWith(regionPath)
                .setTotalNumBuckets(partitionAttributes.getTotalNumBuckets())
                .create())
                .create(fileRegionName);
      }

      // create PR chunkRegion, but not to create its buckets for now
      final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath) + ".chunks";
      chunkRegion = cache.<ChunkKey, byte[]> getRegion(chunkRegionName);
      if (null == chunkRegion) {
        chunkRegion = cache.<ChunkKey, byte[]> createRegionFactory(regionShortCut)
            .setPartitionAttributes(new PartitionAttributesFactory<ChunkKey, byte[]>().setColocatedWith(fileRegionName)
                .setTotalNumBuckets(partitionAttributes.getTotalNumBuckets())
                .create())
                .create(chunkRegionName);
      }

      // we will create RegionDirectorys on the fly when data coming
      HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(getFieldNames());
      repositoryManager = new PartitionedRepositoryManager(dataRegion, (PartitionedRegion)fileRegion, (PartitionedRegion)chunkRegion, mapper, analyzer);
      
      // create AEQ, AEQ listner and specify the listener to repositoryManager
      AsyncEventQueueFactoryImpl factory = (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
      if (withPersistence) {
        factory.setPersistent(true);
      }
      factory.setParallel(true); // parallel AEQ for PR
      factory.setMaximumQueueMemory(1000);
      factory.setDispatcherThreads(1);
      factory.setIsMetaQueue(true);
      
      LuceneEventListener listener = new LuceneEventListener(repositoryManager);
      String aeqId = LuceneServiceImpl.getUniqueIndexName(getName(), regionPath);
      AsyncEventQueueImpl aeq = (AsyncEventQueueImpl)cache.getAsyncEventQueue(aeqId);
      AsyncEventQueue indexQueue = factory.create(aeqId, listener);

      addExtension(dataRegion);
      hasInitialized = true;
    }
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
}
