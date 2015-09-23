package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

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
      if (null == fileRegion) {
        fileRegion = cache.<String, File> createRegionFactory(regionShortCut)
            .setPartitionAttributes(new PartitionAttributesFactory<String, File>().setColocatedWith(regionPath)
                .create())
                .create(fileRegionName);
      }

      // create PR chunkRegion, but not to create its buckets for now
      final String chunkRegionName = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath) + ".chunks";
      chunkRegion = cache.<ChunkKey, byte[]> getRegion(chunkRegionName);
      if (null == chunkRegion) {
        chunkRegion = cache.<ChunkKey, byte[]> createRegionFactory(regionShortCut)
            .setPartitionAttributes(new PartitionAttributesFactory<ChunkKey, byte[]>().setColocatedWith(fileRegionName)
                .create())
                .create(chunkRegionName);
      }

      // we will create RegionDirectorys on the fly when data coming
      HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(getFieldNames());
      repositoryManager = new PartitionedRepositoryManager(dataRegion, (PartitionedRegion)fileRegion, (PartitionedRegion)chunkRegion, mapper, analyzer);
      hasInitialized = true;
    }
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
}
