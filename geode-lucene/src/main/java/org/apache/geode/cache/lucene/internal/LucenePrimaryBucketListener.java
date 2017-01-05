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

import org.apache.geode.GemFireException;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

public class LucenePrimaryBucketListener extends PartitionListenerAdapter {
  private static final Logger logger = LogService.getLogger();
  private PartitionedRepositoryManager lucenePartitionRepositoryManager;
  private final DM dm;

  public LucenePrimaryBucketListener(PartitionedRepositoryManager partitionedRepositoryManager,
      final DM dm) {
    lucenePartitionRepositoryManager = partitionedRepositoryManager;
    this.dm = dm;
  }

  @Override
  public void afterPrimary(int bucketId) {
    dm.getWaitingThreadPool().execute(() -> {
      try {
        lucenePartitionRepositoryManager.getRepository(bucketId);
      } catch (BucketNotFoundException e) {
        logger.warn(
            "Index repository could not be created when index chunk region bucket became primary. "
                + "Deferring index repository to be created lazily during lucene query execution."
                + e);
      }
    });
  }
}
