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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;

public class IndexRepositoryFactory {

  private static final Logger logger = LogService.getLogger();
  public static final String FILE_REGION_LOCK_FOR_BUCKET_ID = "FileRegionLockForBucketId:";
  public static final String APACHE_GEODE_INDEX_COMPLETE = "APACHE_GEODE_INDEX_COMPLETE";

  public IndexRepositoryFactory() {}

  public IndexRepository computeIndexRepository(final Integer bucketId, LuceneSerializer serializer,
      LuceneIndexImpl index, PartitionedRegion userRegion, final IndexRepository oldRepository)
      throws IOException {
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
    final PartitionedRegion fileRegion = indexForPR.getFileAndChunkRegion();

    // We need to ensure that all members have created the fileAndChunk region before continuing
    Region prRoot = PartitionedRegionHelper.getPRRoot(fileRegion.getCache());
    PartitionRegionConfig prConfig =
        (PartitionRegionConfig) prRoot.get(fileRegion.getRegionIdentifier());
    while (!prConfig.isColocationComplete()) {
      prConfig = (PartitionRegionConfig) prRoot.get(fileRegion.getRegionIdentifier());
    }

    BucketRegion fileAndChunkBucket = getMatchingBucket(fileRegion, bucketId);
    BucketRegion dataBucket = getMatchingBucket(userRegion, bucketId);
    boolean success = false;
    if (fileAndChunkBucket == null) {
      if (oldRepository != null) {
        oldRepository.cleanup();
      }
      return null;
    }
    if (!fileAndChunkBucket.getBucketAdvisor().isPrimary()) {
      if (oldRepository != null) {
        oldRepository.cleanup();
      }
      return null;
    }

    if (oldRepository != null && !oldRepository.isClosed()) {
      return oldRepository;
    }

    if (oldRepository != null) {
      oldRepository.cleanup();
    }
    DistributedLockService lockService = getLockService();
    String lockName = getLockName(fileAndChunkBucket);
    while (!lockService.lock(lockName, 100, -1)) {
      if (!fileAndChunkBucket.getBucketAdvisor().isPrimary()) {
        return null;
      }
    }

    final IndexRepository repo;
    DefaultQuery.setPdxReadSerialized(true);
    try {
      // bucketTargetingMap handles partition resolver (via bucketId as callbackArg)
      Map bucketTargetingMap = getBucketTargetingMap(fileAndChunkBucket, bucketId);
      RegionDirectory dir =
          new RegionDirectory(bucketTargetingMap, indexForPR.getFileSystemStats());
      IndexWriterConfig config = new IndexWriterConfig(indexForPR.getAnalyzer());
      IndexWriter writer = new IndexWriter(dir, config);
      repo = new IndexRepositoryImpl(fileAndChunkBucket, writer, serializer,
          indexForPR.getIndexStats(), dataBucket, lockService, lockName, indexForPR);
      success = false;
      // fileRegion ops (get/put) need bucketId as a callbackArg for PartitionResolver
      if (null != fileRegion.get(APACHE_GEODE_INDEX_COMPLETE, bucketId)) {
        success = true;
        return repo;
      } else {
        Set<IndexRepository> affectedRepos = new HashSet<IndexRepository>();

        Iterator keysIterator = dataBucket.keySet().iterator();
        while (keysIterator.hasNext()) {
          Object key = keysIterator.next();
          Object value = getValue(userRegion.getEntry(key));
          if (value != null) {
            repo.update(key, value);
          } else {
            repo.delete(key);
          }
          affectedRepos.add(repo);
        }

        for (IndexRepository affectedRepo : affectedRepos) {
          affectedRepo.commit();
        }
        // fileRegion ops (get/put) need bucketId as a callbackArg for PartitionResolver
        fileRegion.put(APACHE_GEODE_INDEX_COMPLETE, APACHE_GEODE_INDEX_COMPLETE, bucketId);
        success = true;
      }
      return repo;
    } catch (IOException e) {
      logger.info("Exception thrown while constructing Lucene Index for bucket:" + bucketId
          + " for file region:" + fileAndChunkBucket.getFullPath());
      throw e;
    } catch (CacheClosedException e) {
      logger.info("CacheClosedException thrown while constructing Lucene Index for bucket:"
          + bucketId + " for file region:" + fileAndChunkBucket.getFullPath());
      throw e;
    } finally {
      if (!success) {
        lockService.unlock(lockName);
        DefaultQuery.setPdxReadSerialized(false);
      }
    }
  }

  private Object getValue(Region.Entry entry) {
    final EntrySnapshot es = (EntrySnapshot) entry;
    Object value;
    try {
      value = es == null ? null : es.getRawValue(true);
    } catch (EntryDestroyedException e) {
      value = null;
    }
    return value;
  }

  private Map getBucketTargetingMap(BucketRegion region, int bucketId) {
    return new BucketTargetingMap(region, bucketId);
  }

  private String getLockName(final BucketRegion fileAndChunkBucket) {
    return FILE_REGION_LOCK_FOR_BUCKET_ID + fileAndChunkBucket.getFullPath();
  }

  private DistributedLockService getLockService() {
    return DistributedLockService
        .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  protected BucketRegion getMatchingBucket(PartitionedRegion region, Integer bucketId) {
    // Force the bucket to be created if it is not already
    region.getOrCreateNodeForBucketWrite(bucketId, null);

    return region.getDataStore().getLocalBucketById(bucketId);
  }
}
