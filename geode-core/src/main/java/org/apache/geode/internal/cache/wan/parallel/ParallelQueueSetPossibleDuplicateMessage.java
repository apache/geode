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
 * Sets events in the remote secondary queues to possible duplicate
 *
 * @since Geode 1.15
 */
public class ParallelQueueSetPossibleDuplicateMessage extends PooledDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  private int reason;
  private Map<String, Map<Integer, List<Object>>> regionToDuplicateEventsMap;

  public static final int UNSUCCESSFULLY_DISPATCHED = 0;
  public static final int RESET_BATCH = 1;
  public static final int LOAD_FROM_TEMP_QUEUE = 2;
  public static final int STOPPED_GATEWAY_SENDER = 3;


  public ParallelQueueSetPossibleDuplicateMessage() {}

  public ParallelQueueSetPossibleDuplicateMessage(int reason,
      Map<String, Map<Integer, List<Object>>> regionToDuplicateEventsMap) {
    this.reason = reason;
    this.regionToDuplicateEventsMap = regionToDuplicateEventsMap;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_SET_POSSIBLE_DUPLICATE_MESSAGE;
  }

  @Override
  public String toString() {
    String cname = getShortClassName();
    final StringBuilder sb = new StringBuilder(cname);
    sb.append("reason=").append(reason);
    sb.append(" regionToDispatchedKeysMap=").append(regionToDuplicateEventsMap);
    sb.append(" sender=").append(getSender());
    return sb.toString();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final InternalCache cache = dm.getCache();

    if (cache == null) {
      return;
    }
    final InitializationLevel oldLevel =
        LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
    try {
      for (String name : regionToDuplicateEventsMap.keySet()) {
        final PartitionedRegion region = (PartitionedRegion) cache.getRegion(name);
        if (region == null) {
          continue;
        }

        AbstractGatewaySender abstractSender = region.getParallelGatewaySender();
        // Find the map: bucketId to dispatchedKeys
        // Find the bucket
        // Destroy the keys
        Map<Integer, List<Object>> bucketIdToDispatchedKeys =
            this.regionToDuplicateEventsMap.get(name);
        for (Integer bId : bucketIdToDispatchedKeys.keySet()) {
          final String bucketFullPath =
              SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME + SEPARATOR
                  + region.getBucketName(bId);
          BucketRegionQueue brq =
              (BucketRegionQueue) cache.getInternalRegionByPath(bucketFullPath);

          if (brq != null && reason == STOPPED_GATEWAY_SENDER) {
            brq.setReceivedGatewaySenderStoppedMessage(true);
          }

          if (isDebugEnabled) {
            logger.debug(
                "ParallelQueueSetPossibleDuplicateMessage : The bucket in the cache is bucketRegionName : {} bucket: {}",
                bucketFullPath, brq);
          }

          List<Object> dispatchedKeys = bucketIdToDispatchedKeys.get(bId);
          if (dispatchedKeys != null && !dispatchedKeys.isEmpty()) {
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
      } //
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeInteger(this.reason, out);
    DataSerializer.writeHashMap(this.regionToDuplicateEventsMap, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.reason = DataSerializer.readInteger(in);
    this.regionToDuplicateEventsMap = DataSerializer.readHashMap(in);
  }
}
