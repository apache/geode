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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;

import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.logging.LogService;

public class LuceneBucketListener extends PartitionListenerAdapter {
  private static final Logger logger = LogService.getLogger();
  private PartitionedRepositoryManager lucenePartitionRepositoryManager;
  private final DistributionManager dm;

  public LuceneBucketListener(PartitionedRepositoryManager partitionedRepositoryManager,
      final DistributionManager dm) {
    lucenePartitionRepositoryManager = partitionedRepositoryManager;
    this.dm = dm;
  }

  @Override
  public void afterPrimary(int bucketId) {
    dm.getExecutors().getWaitingThreadPool().execute(() -> {
      try {
        lucenePartitionRepositoryManager.computeRepository(bucketId);
      } catch (PrimaryBucketException e) {
        logger.info("Index repository could not be created because we are no longer primary?", e);
      }
    });
  }

  @Override
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
    afterSecondary(bucketId);
  }

  @Override
  public void afterSecondary(int bucketId) {
    dm.getExecutors().getWaitingThreadPool().execute(() -> {
      try {
        lucenePartitionRepositoryManager.computeRepository(bucketId);
      } catch (PrimaryBucketException | AlreadyClosedException e) {
        logger.debug("Exception while cleaning up Lucene Index Repository", e);
      }
    });
  }
}
