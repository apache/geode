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

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BucketId;

public class LuceneRawIndex extends LuceneIndexImpl {

  protected LuceneRawIndex(String indexName, String regionPath, InternalCache cache) {
    super(indexName, regionPath, cache);
  }

  @Override
  protected RepositoryManager createRepositoryManager(LuceneSerializer<?> luceneSerializer) {
    HeterogeneousLuceneSerializer mapper = (HeterogeneousLuceneSerializer) luceneSerializer;
    if (mapper == null) {
      mapper = new HeterogeneousLuceneSerializer();
    }
    return new RawLuceneRepositoryManager(this,
        mapper, cache.getDistributionManager().getExecutors().getWaitingThreadPool());
  }

  @Override
  protected void createLuceneListenersAndFileChunkRegions(
      PartitionedRepositoryManager partitionedRepositoryManager) {
    partitionedRepositoryManager.setUserRegionForRepositoryManager((PartitionedRegion) dataRegion);
  }

  @Override
  public void dumpFiles(String directory) {}

  @Override
  public void destroy(boolean initiator) {}

  @Override
  public boolean isIndexAvailable(BucketId id) {
    return true;
  }

  @Override
  public boolean isIndexingInProgress() {
    return false;
  }
}
