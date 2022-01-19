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

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.size.SingleObjectSizer;

/**
 * Queue built on top of {@link
 * org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue} which allows multiple
 * dispatcher to register and do peek/remove from the underlying {@link
 * org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue}
 *
 * <p>
 * There is only one queue, but this class co-ordinates access by multiple threads such that we
 * get zero contention while peeking or removing.
 *
 * <p>
 * It implements RegionQueue so that AbstractGatewaySenderEventProcessor can work on it.
 */
public class ConcurrentParallelGatewaySenderQueue implements RegionQueue {

  private final AbstractGatewaySender sender;

  private final ParallelGatewaySenderEventProcessor[] processors;

  public ConcurrentParallelGatewaySenderQueue(AbstractGatewaySender sender,
      ParallelGatewaySenderEventProcessor[] pro) {
    this.sender = sender;
    processors = pro;
  }

  @Override
  public boolean put(Object object) throws InterruptedException, CacheException {
    throw new UnsupportedOperationException("CPGAQ method(put) is not supported");
  }

  @Override
  public void close() {
    /*
     * this.commonQueue.close(); // no need to free peekedEvents since they all had makeOffHeap
     * called on them. throw new
     * UnsupportedOperationException("CPGAQ method(close) is not supported");
     */
  }

  @Override
  public Region getRegion() {
    return processors[0].getQueue().getRegion();
  }

  public PartitionedRegion getRegion(String fullpath) {
    return processors[0].getRegion(fullpath);
  }

  public Set<PartitionedRegion> getRegions() {
    return ((ParallelGatewaySenderQueue) (processors[0].getQueue())).getRegions();
  }

  @VisibleForTesting
  public boolean getCleanQueues() {
    return ((ParallelGatewaySenderQueue) (processors[0].getQueue())).getCleanQueues();
  }

  @Override
  public Object take() throws CacheException, InterruptedException {
    throw new UnsupportedOperationException("This method(take) is not supported");
  }

  @Override
  public List take(int batchSize) throws CacheException, InterruptedException {
    throw new UnsupportedOperationException("This method(take) is not supported");
  }

  @Override
  public void remove() throws CacheException {
    throw new UnsupportedOperationException("This method(remove) is not supported");
  }

  @Override
  public Object peek() throws InterruptedException, CacheException {
    throw new UnsupportedOperationException("This method(peek) is not supported");
  }

  @Override
  public List peek(int batchSize) throws InterruptedException, CacheException {
    throw new UnsupportedOperationException("This method(peek) is not supported");
  }

  @Override
  public List peek(int batchSize, int timeToWait) throws InterruptedException, CacheException {
    throw new UnsupportedOperationException("This method(peek) is not supported");
  }

  @Override
  public int size() {
    // is that fine??
    return processors[0].getQueue().size();
  }

  public String displayContent() {
    ParallelGatewaySenderQueue pgsq = (ParallelGatewaySenderQueue) (processors[0].getQueue());
    return pgsq.displayContent();
  }

  public int localSize() {
    return localSize(false);
  }

  public int localSize(boolean includeSecondary) {
    return ((ParallelGatewaySenderQueue) (processors[0].getQueue())).localSize(includeSecondary);
  }

  @Override
  public void addCacheListener(CacheListener listener) {
    processors[0].getQueue().addCacheListener(listener);
  }

  @Override
  public void removeCacheListener() {
    processors[0].removeCacheListener();
  }

  @Override
  public void remove(int top) throws CacheException {
    throw new UnsupportedOperationException("This method(remove) is not suported");
  }

  /*
   * public void resetLastPeeked(){ this.resetLastPeeked = true; }
   */

  public long estimateMemoryFootprint(SingleObjectSizer sizer) {
    long size = 0;
    for (final ParallelGatewaySenderEventProcessor processor : processors) {
      size += ((ParallelGatewaySenderQueue) processor.getQueue())
          .estimateMemoryFootprint(sizer);
    }
    return size;
  }

  public void removeShadowPR(String prRegionName) {
    for (final ParallelGatewaySenderEventProcessor processor : processors) {
      processor.removeShadowPR(prRegionName);
    }
  }

  public void addShadowPartitionedRegionForUserPR(PartitionedRegion pr) {
    // Reset enqueuedAllTempQueueEvents if the sender is running
    // This is done so that any events received while the shadow PR is added are queued in the
    // tmpQueuedEvents
    // instead of blocking the distribute call which could cause a deadlock. See GEM-801.
    if (sender.isRunning()) {
      sender.setEnqueuedAllTempQueueEvents(false);
    }
    sender.getLifeCycleLock().writeLock().lock();
    try {
      for (final ParallelGatewaySenderEventProcessor processor : processors) {
        processor.addShadowPartitionedRegionForUserPR(pr);
      }
    } finally {
      sender.getLifeCycleLock().writeLock().unlock();
    }
  }

  private ParallelGatewaySenderEventProcessor getPGSProcessor(int bucketId) {
    int index = bucketId % processors.length;
    return processors[index];
  }

  public RegionQueue getQueueByBucket(int bucketId) {
    return getPGSProcessor(bucketId).getQueue();
  }

  public BlockingQueue<GatewaySenderEventImpl> getBucketTmpQueue(int bucketId) {
    return getPGSProcessor(bucketId).getBucketTmpQueue(bucketId);
  }

  public void notifyEventProcessorIfRequired(int bucketId) {
    getPGSProcessor(bucketId).notifyEventProcessorIfRequired(bucketId);
  }

  public void clear(PartitionedRegion pr, int bucketId) {
    getPGSProcessor(bucketId).clear(pr, bucketId);
  }

  public void cleanUp() {
    for (final ParallelGatewaySenderEventProcessor processor : processors) {
      ((ParallelGatewaySenderQueue) processor.getQueue()).cleanUp();
    }
  }

  public void conflateEvent(Conflatable conflatableObject, int bucketId, Long tailKey) {
    getPGSProcessor(bucketId).conflateEvent(conflatableObject, bucketId, tailKey);
  }

  public void addShadowPartitionedRegionForUserRR(DistributedRegion userRegion) {
    for (final ParallelGatewaySenderEventProcessor processor : processors) {
      processor.addShadowPartitionedRegionForUserRR(userRegion);
    }
  }

  public long getNumEntriesInVMTestOnly() {
    return ((ParallelGatewaySenderQueue) (processors[0].getQueue())).getNumEntriesInVMTestOnly();
  }

  public long getNumEntriesOverflowOnDiskTestOnly() {
    return ((ParallelGatewaySenderQueue) (processors[0].getQueue()))
        .getNumEntriesOverflowOnDiskTestOnly();
  }
}
