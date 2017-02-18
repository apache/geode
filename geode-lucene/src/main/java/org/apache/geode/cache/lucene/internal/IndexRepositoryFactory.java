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
import java.util.Map;

import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.LuceneSerializer;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

public class IndexRepositoryFactory {

  private static final Logger logger = LogService.getLogger();
  public static final String FILE_REGION_LOCK_FOR_BUCKET_ID = "FileRegionLockForBucketId:";

  public IndexRepositoryFactory() {}

  public IndexRepository computeIndexRepository(final Integer bucketId, LuceneSerializer serializer,
      LuceneIndexImpl index, PartitionedRegion userRegion, final IndexRepository oldRepository)
      throws IOException {
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
    final PartitionedRegion fileRegion = indexForPR.getFileRegion();
    final PartitionedRegion chunkRegion = indexForPR.getChunkRegion();

    BucketRegion fileBucket = getMatchingBucket(fileRegion, bucketId);
    BucketRegion chunkBucket = getMatchingBucket(chunkRegion, bucketId);
    BucketRegion dataBucket = getMatchingBucket(userRegion, bucketId);
    boolean success = false;
    if (fileBucket == null || chunkBucket == null) {
      if (oldRepository != null) {
        oldRepository.cleanup();
      }
      return null;
    }
    if (!chunkBucket.getBucketAdvisor().isPrimary()) {
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
    String lockName = getLockName(bucketId, fileBucket);
    while (!lockService.lock(lockName, 100, -1)) {
      if (!chunkBucket.getBucketAdvisor().isPrimary()) {
        return null;
      }
    }

    final IndexRepository repo;
    try {
      RegionDirectory dir = new RegionDirectory(getBucketTargetingMap(fileBucket, bucketId),
          getBucketTargetingMap(chunkBucket, bucketId), indexForPR.getFileSystemStats());
      IndexWriterConfig config = new IndexWriterConfig(indexForPR.getAnalyzer());
      IndexWriter writer = new IndexWriter(dir, config);
      repo = new IndexRepositoryImpl(fileBucket, writer, serializer, indexForPR.getIndexStats(),
          dataBucket, lockService, lockName);
      success = true;
      return repo;
    } catch (IOException e) {
      logger.info("Exception thrown while constructing Lucene Index for bucket:" + bucketId
          + " for file region:" + fileBucket.getFullPath());
      throw e;
    } finally {
      if (!success) {
        lockService.unlock(lockName);
      }
    }

  }

  private Map getBucketTargetingMap(BucketRegion region, int bucketId) {
    return new BucketTargetingMap(region, bucketId);
  }

  private String getLockName(final Integer bucketId, final BucketRegion fileBucket) {
    return FILE_REGION_LOCK_FOR_BUCKET_ID + fileBucket.getFullPath();
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
