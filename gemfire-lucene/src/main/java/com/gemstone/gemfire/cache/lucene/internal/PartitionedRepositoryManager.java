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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;

/**
 * Manages index repositories for partitioned regions.
 * 
 * This class lazily creates the IndexRepository for each individual
 * bucket. If a Bucket is rebalanced, this class will create a new
 * index repository when the bucket returns to this node.
 */
public class PartitionedRepositoryManager implements RepositoryManager {
  /** map of the parent bucket region to the index repository 
   * 
   * This is based on the BucketRegion in case a bucket is rebalanced, we don't want to 
   * return a stale index repository. If a bucket moves off of this node and
   * comes back, it will have a new BucketRegion object.
   * 
   * It is weak so that the old BucketRegion will be garbage collected. 
   */
  CopyOnWriteHashMap<Integer, IndexRepository> indexRepositories = new CopyOnWriteHashMap<Integer, IndexRepository>();
  
  /** The user region for this index */
  private final PartitionedRegion userRegion;
  
  private final PartitionedRegion fileRegion;
  private final PartitionedRegion chunkRegion;
  private final LuceneSerializer serializer;
  private final Analyzer analyzer;
  
  /**
   * 
   * @param userRegion The user partition region
   * @param fileRegion The partition region used for file metadata. Should be colocated with the user pr
   * @param chunkRegion The partition region users for chunk metadata.
   * @param serializer The serializer that should be used for converting objects to lucene docs.
   */
  public PartitionedRepositoryManager(PartitionedRegion userRegion, PartitionedRegion fileRegion,
      PartitionedRegion chunkRegion,
      LuceneSerializer serializer,
      Analyzer analyzer) {
    this.userRegion = userRegion;
    this.fileRegion = fileRegion;
    this.chunkRegion = chunkRegion;
    this.serializer = serializer;
    this.analyzer = analyzer;
  }

  @Override
  public IndexRepository getRepository(Region region, Object key, Object callbackArg) throws BucketNotFoundException {
    BucketRegion userBucket = userRegion.getBucketRegion(key, callbackArg);
    if(userBucket == null) {
      throw new BucketNotFoundException("User bucket was not found for region " + region + "key " +  key + " callbackarg " + callbackArg);
    }
    
    return getRepository(userBucket.getId());
  }
  
  @Override
  public Collection<IndexRepository> getRepositories(RegionFunctionContext ctx) throws BucketNotFoundException {
    
    Region<Object, Object> region = ctx.getDataSet();
    Set<Integer> buckets = ((InternalRegionFunctionContext) ctx).getLocalBucketSet(region);
    ArrayList<IndexRepository> repos = new ArrayList<IndexRepository>(buckets.size());
    for(Integer bucketId : buckets) {
      BucketRegion userBucket = userRegion.getDataStore().getLocalBucketById(bucketId);
      if(userBucket == null) {
        throw new BucketNotFoundException("User bucket was not found for region " + region + "bucket id " + bucketId);
      } else {
        repos.add(getRepository(userBucket.getId()));
      }
    }

    return repos;
  }

  /**
   * Return the repository for a given user bucket
   */
  private IndexRepository getRepository(Integer bucketId) throws BucketNotFoundException {
    IndexRepository repo = indexRepositories.get(bucketId);
    
    //Remove the repository if it has been destroyed (due to rebalancing)
    if(repo != null && repo.isClosed()) {
      indexRepositories.remove(bucketId, repo);
      repo = null;
    }
    
    if(repo == null) {
      try {
        BucketRegion fileBucket = getMatchingBucket(fileRegion, bucketId);
        BucketRegion chunkBucket = getMatchingBucket(chunkRegion, bucketId);
        RegionDirectory dir = new RegionDirectory(fileBucket, chunkBucket);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, config);
        repo = new IndexRepositoryImpl(fileBucket, writer, serializer);
        IndexRepository oldRepo = indexRepositories.putIfAbsent(bucketId, repo);
        if(oldRepo != null) {
          repo = oldRepo;
        }
      } catch(IOException e) {
        throw new InternalGemFireError("Unable to create index repository", e);
      }
    }
    
    return repo;
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  private BucketRegion getMatchingBucket(PartitionedRegion region, Integer bucketId) throws BucketNotFoundException {
    //Force the bucket to be created if it is not already
    region.getOrCreateNodeForBucketWrite(bucketId, null);
    
    BucketRegion result = region.getDataStore().getLocalBucketById(bucketId);
    if(result == null) {
      throw new BucketNotFoundException("Bucket not found for region " + region + " bucekt id " + bucketId);
    }
    
    return result;
  }
}
