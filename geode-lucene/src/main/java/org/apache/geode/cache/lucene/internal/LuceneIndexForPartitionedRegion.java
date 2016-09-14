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

package org.apache.geode.cache.lucene.internal;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
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
import org.apache.geode.cache.lucene.internal.filesystem.ChunkKey;
import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;

/* wrapper of IndexWriter */
public class LuceneIndexForPartitionedRegion extends LuceneIndexImpl {
  protected Region<String, File> fileRegion;
  protected Region<ChunkKey, byte[]> chunkRegion;
  protected final FileSystemStats fileSystemStats;

  public LuceneIndexForPartitionedRegion(String indexName, String regionPath, Cache cache) {
    super(indexName, regionPath, cache);

    final String statsName = indexName + "-" + regionPath;
    this.fileSystemStats = new FileSystemStats(cache.getDistributedSystem(), statsName);
  }

  protected RepositoryManager createRepositoryManager() {
    RegionShortcut regionShortCut;
    final boolean withPersistence = withPersistence(); 
    RegionAttributes regionAttributes = dataRegion.getAttributes();
    final boolean withStorage = regionAttributes.getPartitionAttributes().getLocalMaxMemory()>0;

    // TODO: 1) dataRegion should be withStorage
    //       2) Persistence to Persistence
    //       3) Replicate to Replicate, Partition To Partition
    //       4) Offheap to Offheap
    if (!withStorage) {
      throw new IllegalStateException("The data region to create lucene index should be with storage");
    }
    if (withPersistence) {
      // TODO: add PartitionedRegionAttributes instead
      regionShortCut = RegionShortcut.PARTITION_PERSISTENT;
    } else {
      regionShortCut = RegionShortcut.PARTITION;
    }
    
    // create PR fileRegion, but not to create its buckets for now
    final String fileRegionName = createFileRegionName();
    PartitionAttributes partitionAttributes = dataRegion.getPartitionAttributes();
    if (!fileRegionExists(fileRegionName)) {
      fileRegion = createFileRegion(regionShortCut, fileRegionName, partitionAttributes, regionAttributes);
    }

    // create PR chunkRegion, but not to create its buckets for now
    final String chunkRegionName = createChunkRegionName();
    if (!chunkRegionExists(chunkRegionName)) {
      chunkRegion = createChunkRegion(regionShortCut, fileRegionName, partitionAttributes, chunkRegionName, regionAttributes);
    }
    fileSystemStats.setFileSupplier(() -> (int) getFileRegion().getLocalSize());
    fileSystemStats.setChunkSupplier(() -> (int) getChunkRegion().getLocalSize());
    fileSystemStats.setBytesSupplier(() -> getChunkRegion().getPrStats().getDataStoreBytesInUse());

    // we will create RegionDirectories on the fly when data comes in
    HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(getFieldNames());
    return new PartitionedRepositoryManager(this, mapper);
  }
  
  public PartitionedRegion getFileRegion() {
    return (PartitionedRegion) fileRegion;
  }

  public PartitionedRegion getChunkRegion() {
    return (PartitionedRegion) chunkRegion;
  }

  public FileSystemStats getFileSystemStats() {
    return fileSystemStats;
  }
  
  boolean fileRegionExists(String fileRegionName) {
    return cache.<String, File> getRegion(fileRegionName) != null;
  }

  Region createFileRegion(final RegionShortcut regionShortCut,
                                final String fileRegionName,
                                final PartitionAttributes partitionAttributes,
                                final RegionAttributes regionAttributes) {
    return createRegion(fileRegionName, regionShortCut, this.regionPath, partitionAttributes, regionAttributes);
  }

  public String createFileRegionName() {
    return LuceneServiceImpl.getUniqueIndexName(indexName, regionPath)+".files";
  }

  boolean chunkRegionExists(String chunkRegionName) {
    return cache.<ChunkKey, byte[]> getRegion(chunkRegionName) != null;
  }

  Region<ChunkKey, byte[]> createChunkRegion(final RegionShortcut regionShortCut,
                           final String fileRegionName,
                           final PartitionAttributes partitionAttributes, final String chunkRegionName, final RegionAttributes regionAttributes) {
    return createRegion(chunkRegionName, regionShortCut, fileRegionName, partitionAttributes, regionAttributes);
  }

  public String createChunkRegionName() {
    return LuceneServiceImpl.getUniqueIndexName(indexName, regionPath) + ".chunks";
  }

  private PartitionAttributesFactory configureLuceneRegionAttributesFactory(PartitionAttributesFactory attributesFactory, PartitionAttributes<?,?> dataRegionAttributes) {
    attributesFactory.setTotalNumBuckets(dataRegionAttributes.getTotalNumBuckets());
    attributesFactory.setRedundantCopies(dataRegionAttributes.getRedundantCopies());
    return attributesFactory;
  }

  protected <K, V> Region<K, V> createRegion(final String regionName,
                                             final RegionShortcut regionShortCut,
                                             final String colocatedWithRegionName,
                                             final PartitionAttributes partitionAttributes,
                                             final RegionAttributes regionAttributes)
  {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory<String, File>();
    partitionAttributesFactory.setColocatedWith(colocatedWithRegionName);
    configureLuceneRegionAttributesFactory(partitionAttributesFactory, partitionAttributes);

    // Create AttributesFactory based on input RegionShortcut
    RegionAttributes baseAttributes = this.cache.getRegionAttributes(regionShortCut.toString());
    AttributesFactory factory = new AttributesFactory(baseAttributes);
    factory.setPartitionAttributes(partitionAttributesFactory.create());
    factory.setDiskStoreName(regionAttributes.getDiskStoreName());
    RegionAttributes<K, V> attributes = factory.create();

    return createRegion(regionName, attributes);
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void dumpFiles(final String directory) {
    ResultCollector results = FunctionService.onRegion(getDataRegion())
      .withArgs(new String[] {directory, indexName})
      .execute(DumpDirectoryFiles.ID);
    results.getResult();
  }
}
