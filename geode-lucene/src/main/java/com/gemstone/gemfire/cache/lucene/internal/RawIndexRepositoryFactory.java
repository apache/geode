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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.gemstone.gemfire.cache.lucene.internal.directory.RegionDirectory;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepositoryImpl;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class RawIndexRepositoryFactory extends IndexRepositoryFactory {
  public RawIndexRepositoryFactory() {
  }

  public IndexRepository createIndexRepository(final Integer bucketId,
                                        LuceneSerializer serializer,
                                        LuceneIndexImpl index, PartitionedRegion userRegion)
    throws IOException
  {
    final IndexRepository repo;
    LuceneRawIndex indexForRaw = (LuceneRawIndex)index;
    BucketRegion dataBucket = getMatchingBucket(userRegion, bucketId);

    Directory dir = null;
    if (indexForRaw.withPersistence()) {
      String bucketLocation = LuceneServiceImpl.getUniqueIndexName(index.getName(), index.getRegionPath()+"_"+bucketId);
      File location = new File(index.getName(), bucketLocation);
      if (!location.exists()) {
        location.mkdirs();
      }
      dir = new NIOFSDirectory(location.toPath());
    } else {
      dir = new RAMDirectory();
    }
    IndexWriterConfig config = new IndexWriterConfig(indexForRaw.getAnalyzer());
    IndexWriter writer = new IndexWriter(dir, config);
    return new IndexRepositoryImpl(null, writer, serializer, indexForRaw.getIndexStats(), dataBucket);
  }
}
