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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Removes a batch of events from the remote secondary queues
 *
 * @since GemFire 8.0
 */
public class ParallelQueueSetPossibleDuplicateMessage extends PooledDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  private Map regionToDispatchedKeysMap;

  public ParallelQueueSetPossibleDuplicateMessage() {}

  public ParallelQueueSetPossibleDuplicateMessage(Map rgnToDispatchedKeysMap) {
    this.regionToDispatchedKeysMap = rgnToDispatchedKeysMap;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_SET_POSSIBLE_DUPLICATE_MESSAGE;
  }

  @Override
  public String toString() {
    String cname = getShortClassName();
    final StringBuilder sb = new StringBuilder(cname);
    sb.append("regionToDispatchedKeysMap=" + regionToDispatchedKeysMap);
    sb.append(" sender=").append(getSender());
    return sb.toString();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final InternalCache cache = dm.getCache();
    logger.info("ParallelQueueSetPossibleDuplicateMessage received {}.", regionToDispatchedKeysMap);

    if (cache != null) {
      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        for (Object name : regionToDispatchedKeysMap.keySet()) {
          final String regionName = (String) name;
          final PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
          if (region == null) {
            continue;
          } else {
            AbstractGatewaySender abstractSender = region.getParallelGatewaySender();
            // Find the map: bucketId to dispatchedKeys
            // Find the bucket
            // Destroy the keys
            Map bucketIdToDispatchedKeys = (Map) this.regionToDispatchedKeysMap.get(regionName);
            for (Object bId : bucketIdToDispatchedKeys.keySet()) {
              final String bucketFullPath =
                  SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME + SEPARATOR
                      + region.getBucketName((Integer) bId);
              BucketRegionQueue brq =
                  (BucketRegionQueue) cache.getInternalRegionByPath(bucketFullPath);
              if (isDebugEnabled) {
                logger.debug(
                    "ParallelQueueSetPossibleDuplicateMessage : The bucket in the cache is bucketRegionName : {} bucket: {}",
                    bucketFullPath, brq);
              }

              List dispatchedKeys = (List) bucketIdToDispatchedKeys.get((Integer) bId);
              if (dispatchedKeys != null) {
                for (Object key : dispatchedKeys) {
                  // First, clear the Event from tempQueueEvents at AbstractGatewaySender level, if
                  // exists
                  // synchronize on AbstractGatewaySender.queuedEventsSync while doing so
                  abstractSender.markAsDuplicateInTempQueueEvents(key);

                  if (brq != null) {
                    if (isDebugEnabled) {
                      logger.debug(
                          "ParallelQueueSetPossibleDuplicateMessage : The bucket {} key {}.",
                          brq, key);
                    }

                    if (brq.checkIfQueueContainsKey(key)) {
                      brq.setAsPossibleDuplicate(key);
                    }
                  }
                }
              }
            }
          }
        } //
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    } // cache != null
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeHashMap(this.regionToDispatchedKeysMap, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.regionToDispatchedKeysMap = DataSerializer.readHashMap(in);
  }
}
