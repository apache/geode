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

import com.gemstone.gemfire.cache.lucene.internal.repository.IndexRepository;
import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.LuceneSerializer;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class RawLuceneRepositoryManager extends AbstractPartitionedRepositoryManager {
  public static IndexRepositoryFactory indexRepositoryFactory = new RawIndexRepositoryFactory();
  
  public RawLuceneRepositoryManager(LuceneIndexImpl index,
      LuceneSerializer serializer) {
    super(index, serializer);
  }
  
  public void close() {
    for (IndexRepository repo:indexRepositories.values()) {
      repo.cleanup();
    }
  }

  @Override
  public IndexRepository createOneIndexRepository(Integer bucketId,
      LuceneSerializer serializer, LuceneIndexImpl index,
      PartitionedRegion userRegion) throws IOException {
    return indexRepositoryFactory.createIndexRepository(bucketId, serializer, index, userRegion);
  }
}
