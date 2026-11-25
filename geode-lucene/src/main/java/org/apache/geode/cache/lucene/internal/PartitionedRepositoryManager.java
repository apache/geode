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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.lucene.LuceneIndexDestroyedException;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PartitionedRepositoryManager implements RepositoryManager {
  private final Logger logger = LogService.getLogger();

  public static IndexRepositoryFactory indexRepositoryFactory = new IndexRepositoryFactory();
  /**
   * map of the parent bucket region to the index repository
   *
   * This is based on the BucketRegion in case a bucket is rebalanced, we don't want to return a
   * stale index repository. If a bucket moves off of this node and comes back, it will have a new
   * BucketRegion object.
   *
   * It is weak so that the old BucketRegion will be garbage collected.
   */
  protected final ConcurrentHashMap<Integer, IndexRepository> indexRepositories =
      new ConcurrentHashMap<>();

  /** The user region for this index */
  protected PartitionedRegion userRegion = null;
  protected final LuceneSerializer serializer;
  protected final InternalLuceneIndex index;
  protected volatile boolean closed;
  private final CountDownLatch isDataRegionReady = new CountDownLatch(1);

  private final ExecutorService waitingThreadPoolFromDM;

  public PartitionedRepositoryManager(InternalLuceneIndex index, LuceneSerializer serializer,
      ExecutorService waitingThreadPool) {
    this.index = index;
    this.serializer = serializer;
    closed = false;
    waitingThreadPoolFromDM = waitingThreadPool;
  }

  public void setUserRegionForRepositoryManager(PartitionedRegion userRegion) {
    this.userRegion = userRegion;
  }

  public InternalLuceneIndex getIndex() {
    return index;
  }

  /**
   * Returns a map from local primary bucket ID to its {@link IndexRepository} for the buckets
   * targeted by {@code ctx}.
   */
  public Map<Integer, IndexRepository> getRepositoriesByBucket(RegionFunctionContext ctx,
      boolean waitForRepository) throws BucketNotFoundException {
    Region<Object, Object> region = ctx.getDataSet();
    int[] buckets = ((InternalRegionFunctionContext) ctx).getLocalBucketArray(region);
    if (buckets == null || buckets[0] == 0) {
      return Collections.emptyMap();
    }
    Map<Integer, IndexRepository> reposByBucket = new LinkedHashMap<>(buckets[0]);
    for (int i = 1; i <= buckets[0]; i++) {
      int bucketId = buckets[i];
      BucketRegion userBucket = userRegion.getDataStore().getLocalBucketById(bucketId);
      if (userBucket == null) {
        throw new BucketNotFoundException(
            "User bucket was not found for region " + region + "bucket id " + bucketId);
      } else {
        if (index.isIndexAvailable(userBucket.getId()) || userBucket.isEmpty()
            || waitForRepository) {
          reposByBucket.put(userBucket.getId(), getRepository(userBucket.getId()));
        } else {
          waitingThreadPoolFromDM.execute(() -> {
            try {
              getRepository(userBucket.getId());
            } catch (Exception e) {
              logger.debug("Lucene Index creation in progress.", e);
            }
          });
          throw new LuceneIndexCreationInProgressException(
              "Lucene Index creation in progress for bucket: " + userBucket.getId());
        }
      }
    }
    return reposByBucket;
  }

  @Override
  public Collection<IndexRepository> getRepositories(RegionFunctionContext ctx)
      throws BucketNotFoundException {
    return getRepositories(ctx, false);
  }

  @Override
  public Collection<IndexRepository> getRepositories(RegionFunctionContext ctx,
      boolean waitForRepository) throws BucketNotFoundException {
    Region<Object, Object> region = ctx.getDataSet();
    int[] buckets = ((InternalRegionFunctionContext) ctx).getLocalBucketArray(region);
    if (buckets == null || buckets[0] == 0) {
      return null;
    }
    ArrayList<IndexRepository> repos = new ArrayList<>(buckets[0]);
    for (int i = 1; i <= buckets[0]; i++) {
      int bucketId = buckets[i];
      BucketRegion userBucket = userRegion.getDataStore().getLocalBucketById(bucketId);
      if (userBucket == null) {
        throw new BucketNotFoundException(
            "User bucket was not found for region " + region + "bucket id " + bucketId);
      } else {
        if (index.isIndexAvailable(userBucket.getId()) || userBucket.isEmpty()
            || waitForRepository) {
          repos.add(getRepository(userBucket.getId()));
        } else {
          waitingThreadPoolFromDM.execute(() -> {
            try {
              getRepository(userBucket.getId());
            } catch (Exception e) {
              logger.debug("Lucene Index creation in progress.", e);
            }
          });
          throw new LuceneIndexCreationInProgressException(
              "Lucene Index creation in progress for bucket: " + userBucket.getId());
        }
      }
    }

    return repos;
  }

  @Override
  public IndexRepository getRepository(Region region, Object key, Object callbackArg)
      throws BucketNotFoundException {
    BucketRegion userBucket = userRegion.getBucketRegion(key, callbackArg);
    if (userBucket == null) {
      throw new BucketNotFoundException("User bucket was not found for region " + region + "key "
          + key + " callbackarg " + callbackArg);
    }

    return getRepository(userBucket.getId());
  }

  /**
   * Return the repository for a given user bucket
   */
  protected IndexRepository getRepository(Integer bucketId) throws BucketNotFoundException {
    IndexRepository repo = indexRepositories.get(bucketId);
    if (repo != null && !repo.isClosed()) {
      return repo;
    }

    repo = computeRepository(bucketId);

    if (repo == null) {
      throw new BucketNotFoundException(
          "Unable to find lucene index because no longer primary for bucket " + bucketId);
    }
    return repo;
  }

  /**
   * Collects keys currently committed in the local Lucene index for {@code bucketId} without
   * blocking on colocated child {@code BucketAdvisor.getPrimary()} transitions (such as during
   * {@code PartitionListener.afterPrimary(bucketId)} before colocated file-region primary
   * promotion has completed).
   */
  public Set<Object> collectIndexedKeysIfReady(int bucketId)
      throws BucketNotFoundException, IOException {
    if (closed || isDataRegionReady.getCount() > 0L) {
      return Collections.emptySet();
    }
    IndexRepository cached = indexRepositories.get(bucketId);
    if (cached != null && !cached.isClosed()) {
      return cached.collectIndexedKeys();
    }
    if (index instanceof LuceneIndexForPartitionedRegion) {
      LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion) index;
      PartitionedRegion fileRegion = indexForPR.getFileAndChunkRegion();
      if (fileRegion == null || fileRegion.getDataStore() == null) {
        return Collections.emptySet();
      }
      BucketRegion fileBucket = fileRegion.getDataStore().getLocalBucketById(bucketId);
      if (fileBucket == null || fileBucket.isDestroyed() || fileBucket.isEmpty()) {
        return Collections.emptySet();
      }
      if (!fileBucket.getBucketAdvisor().isPrimary()) {
        return IndexRepositoryImpl.collectIndexedKeys(
            fileBucket, bucketId, indexForPR.getFileSystemStats());
      }
    }
    IndexRepository repo = getRepositoryIfReady(bucketId);
    if (repo != null && !repo.isClosed()) {
      return repo.collectIndexedKeys();
    }
    return Collections.emptySet();
  }

  /**
   * Resolves the local bucket's {@link IndexRepository} during primary promotion fallback
   * reconciliation if data regions are ready and an index is available or already initialized,
   * without blocking on remote primary lookup for unpromoted colocated file buckets.
   */
  public IndexRepository getRepositoryIfReady(int bucketId) throws BucketNotFoundException {
    if (closed || isDataRegionReady.getCount() > 0L) {
      return null;
    }
    IndexRepository repo = indexRepositories.get(bucketId);
    if (repo != null && !repo.isClosed()) {
      return repo;
    }
    if (index instanceof LuceneIndexForPartitionedRegion) {
      PartitionedRegion fileRegion =
          ((LuceneIndexForPartitionedRegion) index).getFileAndChunkRegion();
      if (fileRegion == null || fileRegion.getDataStore() == null) {
        return null;
      }
      BucketRegion fileBucket = fileRegion.getDataStore().getLocalBucketById(bucketId);
      if (fileBucket == null || fileBucket.isDestroyed()
          || !fileBucket.getBucketAdvisor().isPrimary()
          || fileBucket.getRegionMap().getEntry(
              IndexRepositoryFactory.APACHE_GEODE_INDEX_COMPLETE) == null) {
        return null;
      }
      return getRepository(bucketId);
    }
    if (index != null && index.isIndexAvailable(bucketId)) {
      return getRepository(bucketId);
    }
    return null;
  }

  protected IndexRepository computeRepository(Integer bucketId, LuceneSerializer serializer,
      InternalLuceneIndex index, PartitionedRegion userRegion, IndexRepository oldRepository)
      throws IOException {
    return indexRepositoryFactory.computeIndexRepository(bucketId, serializer, index, userRegion,
        oldRepository, this);
  }


  protected IndexRepository computeRepository(Integer bucketId) {
    try {
      isDataRegionReady.await();
    } catch (InterruptedException e) {
      throw new InternalGemFireError("Unable to create index repository", e);
    }
    IndexRepository repo = indexRepositories.compute(bucketId, (key, oldRepository) -> {
      try {
        if (closed) {
          if (oldRepository != null) {
            oldRepository.cleanup();
          }
          throw new LuceneIndexDestroyedException(index.getName(), index.getRegionPath());
        }
        return computeRepository(bucketId, serializer, index, userRegion, oldRepository);
      } catch (IOException e) {
        throw new InternalGemFireError("Unable to create index repository", e);
      }
    });
    return repo;
  }

  protected void allowRepositoryComputation() {
    isDataRegionReady.countDown();
  }


  @Override
  public void close() {
    closed = true;
    for (Integer bucketId : indexRepositories.keySet()) {
      try {
        computeRepository(bucketId);
      } catch (LuceneIndexDestroyedException e) {
        /* expected exception */}
    }
  }
}
