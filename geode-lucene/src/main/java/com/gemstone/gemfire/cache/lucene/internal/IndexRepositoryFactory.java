/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal;

import java.io.IOException;

import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.FileSystemStats;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

public class IndexRepositoryFactory {

  public IndexRepositoryFactory() {
  }

  public IndexRepository createIndexRepository(final Integer bucketId,
                                        PartitionedRegion userRegion,
                                        PartitionedRegion fileRegion,
                                        PartitionedRegion chunkRegion,
                                        LuceneSerializer serializer,
                                        Analyzer analyzer,
                                        LuceneIndexStats indexStats,
                                        FileSystemStats fileSystemStats)
    throws IOException
  {
    final IndexRepository repo;
    BucketRegion fileBucket = getMatchingBucket(fileRegion, bucketId);
    BucketRegion chunkBucket = getMatchingBucket(chunkRegion, bucketId);
    if(fileBucket == null || chunkBucket == null) {
      return null;
    }
    RegionDirectory dir = new RegionDirectory(fileBucket, chunkBucket, fileSystemStats);
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    IndexWriter writer = new IndexWriter(dir, config);
    repo = new IndexRepositoryImpl(fileBucket, writer, serializer, indexStats);
    return repo;
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  private BucketRegion getMatchingBucket(PartitionedRegion region, Integer bucketId) {
    //Force the bucket to be created if it is not already
    region.getOrCreateNodeForBucketWrite(bucketId, null);

    return region.getDataStore().getLocalBucketById(bucketId);
  }
}