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
package org.apache.geode.cache.lucene.internal;

import java.io.IOException;

import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.LuceneSerializer;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

public class IndexRepositoryFactory {

  public IndexRepositoryFactory() {
  }

  public IndexRepository createIndexRepository(final Integer bucketId,
                                        LuceneSerializer serializer,
                                        LuceneIndexImpl index, PartitionedRegion userRegion)
    throws IOException
  {
    final IndexRepository repo;
    LuceneIndexForPartitionedRegion indexForPR = (LuceneIndexForPartitionedRegion)index; 
    BucketRegion fileBucket = getMatchingBucket(indexForPR.getFileRegion(), bucketId);
    BucketRegion chunkBucket = getMatchingBucket(indexForPR.getChunkRegion(), bucketId);
    BucketRegion dataBucket = getMatchingBucket(userRegion, bucketId);
    if(fileBucket == null || chunkBucket == null) {
      return null;
    }
    RegionDirectory dir = new RegionDirectory(fileBucket, chunkBucket, indexForPR.getFileSystemStats());
    IndexWriterConfig config = new IndexWriterConfig(indexForPR.getAnalyzer());
    IndexWriter writer = new IndexWriter(dir, config);
    repo = new IndexRepositoryImpl(fileBucket, writer, serializer, indexForPR.getIndexStats(), dataBucket);
    return repo;
  }

  /**
   * Find the bucket in region2 that matches the bucket id from region1.
   */
  protected BucketRegion getMatchingBucket(PartitionedRegion region, Integer bucketId) {
    //Force the bucket to be created if it is not already
    region.getOrCreateNodeForBucketWrite(bucketId, null);

    return region.getDataStore().getLocalBucketById(bucketId);
  }
}