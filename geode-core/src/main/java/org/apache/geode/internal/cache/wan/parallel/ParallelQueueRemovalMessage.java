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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.logging.LogService;

/**
 * Removes a batch of events from the remote secondary queues
 *
 * @since GemFire 8.0
 */
public class ParallelQueueRemovalMessage extends PooledDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  private HashMap regionToDispatchedKeysMap;

  public ParallelQueueRemovalMessage() {}

  public ParallelQueueRemovalMessage(HashMap rgnToDispatchedKeysMap) {
    this.regionToDispatchedKeysMap = rgnToDispatchedKeysMap;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_REMOVAL_MESSAGE;
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
    if (cache != null) {
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
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
                  Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME + Region.SEPARATOR
                      + region.getBucketName((Integer) bId);
              AbstractBucketRegionQueue brq =
                  (AbstractBucketRegionQueue) cache.getRegionByPath(bucketFullPath);
              if (isDebugEnabled) {
                logger.debug(
                    "ParallelQueueRemovalMessage : The bucket in the cache is bucketRegionName : {} bucket: {}",
                    bucketFullPath, brq);
              }

              List dispatchedKeys = (List) bucketIdToDispatchedKeys.get((Integer) bId);
              if (dispatchedKeys != null) {
                for (Object key : dispatchedKeys) {
                  // First, clear the Event from tempQueueEvents at AbstractGatewaySender level, if
                  // exists
                  // synchronize on AbstractGatewaySender.queuedEventsSync while doing so
                  abstractSender.removeFromTempQueueEvents(key);

                  if (brq != null) {
                    if (brq.isInitialized()) {
                      if (isDebugEnabled) {
                        logger.debug(
                            "ParallelQueueRemovalMessage : The bucket {} is initialized. Destroying the key {} from BucketRegionQueue.",
                            bucketFullPath, key);
                      }
                      // fix for #48082
                      afterAckForSecondary_EventInBucket(abstractSender, brq, key);
                      destroyKeyFromBucketQueue(brq, key, region);
                    } else {
                      // if bucket is not initialized, the event should either be in bucket or
                      // tempQueue
                      boolean isDestroyed = false;
                      if (isDebugEnabled) {
                        logger.debug(
                            "ParallelQueueRemovalMessage : The bucket {} is not yet initialized.",
                            bucketFullPath);
                      }
                      brq.getInitializationLock().readLock().lock();
                      try {
                        if (brq.containsKey(key)) {
                          // fix for #48082
                          afterAckForSecondary_EventInBucket(abstractSender, brq, key);
                          destroyKeyFromBucketQueue(brq, key, region);
                          isDestroyed = true;
                        }

                        // Even if BucketRegionQueue does not have the key, it could be in the
                        // tempQueue
                        // remove it from there..defect #49196
                        destroyFromTempQueue(brq.getPartitionedRegion(), (Integer) bId, key);

                        // Finally, add the key to the failed batch removal keys so that it is
                        // definitely removed from the bucket region queue
                        brq.addToFailedBatchRemovalMessageKeys(key);
                      } finally {
                        brq.getInitializationLock().readLock().unlock();
                      }
                    }
                  } else {// brq is null. Destroy the event from tempQueue. Defect #49196
                    destroyFromTempQueue(region, (Integer) bId, key);
                  }
                }
              }
            }
          }
        } // for loop regionToDispatchedKeysMap.keySet()
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    } // cache != null
  }

  // fix for #48082
  private void afterAckForSecondary_EventInBucket(AbstractGatewaySender abstractSender,
      AbstractBucketRegionQueue brq, Object key) {
    for (GatewayEventFilter filter : abstractSender.getGatewayEventFilters()) {
      GatewayQueueEvent eventForFilter = (GatewayQueueEvent) brq.get(key);
      try {
        if (eventForFilter != null) {
          filter.afterAcknowledgement(eventForFilter);
        }
      } catch (Exception e) {
        logger.fatal(String.format(
            "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
            new Object[] {filter.toString(), eventForFilter}),
            e);
      }
    }
  }

  void destroyKeyFromBucketQueue(AbstractBucketRegionQueue brq, Object key,
      PartitionedRegion prQ) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      brq.destroyKey(key);
      if (!brq.getBucketAdvisor().isPrimary()) {
        prQ.getParallelGatewaySender().getStatistics().decSecondaryQueueSize();
        prQ.getParallelGatewaySender().getStatistics().incEventsProcessedByPQRM(1);
      }
      if (isDebugEnabled) {
        logger.debug("Destroyed the key {} for shadowPR {} for bucket {}", key, prQ.getName(),
            brq.getId());
      }
    } catch (EntryNotFoundException e) {
      if (isDebugEnabled) {
        logger.debug("Got EntryNotFoundException while destroying the key {} for bucket {}", key,
            brq.getId());
      }
      // add the key to failedBatchRemovalMessageQueue.
      // This is to handle the last scenario in #49196
      // But if GII is already completed and FailedBatchRemovalMessageKeys
      // are already cleared then no keys should be added to it as they will
      // never be cleared and increase the memory footprint.
      if (!brq.isFailedBatchRemovalMessageKeysClearedFlag()) {
        brq.addToFailedBatchRemovalMessageKeys(key);
      }
    } catch (ForceReattemptException fe) {
      if (isDebugEnabled) {
        logger.debug(
            "Got ForceReattemptException while getting bucket {} to destroyLocally the keys.",
            brq.getId());
      }
    } catch (CancelException e) {
      return; // cache or DS is closing
    } catch (CacheException e) {
      logger.error(String.format(
          "ParallelQueueRemovalMessage::process:Exception in processing the last disptached key for a ParallelGatewaySenderQueue's shadowPR. The problem is with key,%s for shadowPR with name=%s",
          new Object[] {key, prQ.getName()}),
          e);
    }
  }

  private boolean destroyFromTempQueue(PartitionedRegion qPR, int bId, Object key) {
    boolean isDestroyed = false;
    Set queues = qPR.getParallelGatewaySender().getQueues();
    if (queues != null) {
      ConcurrentParallelGatewaySenderQueue prq =
          (ConcurrentParallelGatewaySenderQueue) queues.toArray()[0];
      BlockingQueue<GatewaySenderEventImpl> tempQueue = prq.getBucketTmpQueue(bId);
      if (tempQueue != null) {
        Iterator<GatewaySenderEventImpl> itr = tempQueue.iterator();
        while (itr.hasNext()) {
          GatewaySenderEventImpl eventForFilter = itr.next();
          // fix for #48082
          afterAckForSecondary_EventInTempQueue(qPR.getParallelGatewaySender(), eventForFilter);
          if (eventForFilter.getShadowKey().equals(key)) {
            itr.remove();
            eventForFilter.release(); // GEODE-1282
            isDestroyed = true;
          }
        }
      }
    }
    return isDestroyed;
  }

  // fix for #48082
  private void afterAckForSecondary_EventInTempQueue(
      AbstractGatewaySender parallelGatewaySenderImpl, GatewaySenderEventImpl eventForFilter) {
    for (GatewayEventFilter filter : parallelGatewaySenderImpl.getGatewayEventFilters()) {
      try {
        if (eventForFilter != null) {
          filter.afterAcknowledgement(eventForFilter);
        }
      } catch (Exception e) {
        logger.fatal(String.format(
            "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
            new Object[] {filter.toString(), eventForFilter}),
            e);
      }
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeHashMap(this.regionToDispatchedKeysMap, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionToDispatchedKeysMap = DataSerializer.readHashMap(in);
  }
}
