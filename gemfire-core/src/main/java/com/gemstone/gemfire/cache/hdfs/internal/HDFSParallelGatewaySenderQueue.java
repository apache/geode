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

package com.gemstone.gemfire.cache.hdfs.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;

/**
 * Parallel Gateway Sender Queue extended for HDFS functionality 
 *
 * @author Hemant Bhanawat
 */
public class HDFSParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

  private int currentBucketIndex = 0;
  private int elementsPeekedAcrossBuckets = 0;
  private SystemTimer rollListTimer = null;
  public static final String ROLL_SORTED_LIST_TIME_INTERVAL_MS__PROP = "gemfire.ROLL_SORTED_LIST_TIME_INTERVAL_MS";
  private final int ROLL_SORTED_LIST_TIME_INTERVAL_MS = Integer.getInteger(ROLL_SORTED_LIST_TIME_INTERVAL_MS__PROP, 3000);
  
  public HDFSParallelGatewaySenderQueue(AbstractGatewaySender sender,
      Set<Region> userPRs, int idx, int nDispatcher) {
     
    super(sender, userPRs, idx, nDispatcher);
    //only first dispatcher Hemant?
    if (sender.getBucketSorted() && this.index == 0) {
      rollListTimer = new SystemTimer(sender.getCache().getDistributedSystem(),
          true);
      // schedule the task to roll the skip lists
      rollListTimer.scheduleAtFixedRate(new RollSortedListsTimerTask(), 
          ROLL_SORTED_LIST_TIME_INTERVAL_MS, ROLL_SORTED_LIST_TIME_INTERVAL_MS);
    }
  }
  
  @Override
  public Object peek() throws InterruptedException, CacheException {
    /* If you call peek and use super.peek it leads to the following exception.
     * So I'm adding an explicit UnsupportedOperationException.
     Caused by: java.lang.ClassCastException: com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue cannot be cast to com.gemstone.gemfire.internal.cache.BucketRegionQueue
        at com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue.getRandomPrimaryBucket(ParallelGatewaySenderQueue.java:964)
        at com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue.peek(ParallelGatewaySenderQueue.java:1078)
     */
    throw new UnsupportedOperationException();
  }
  
  
  @Override
  public void cleanUp() {
    super.cleanUp();
    cancelRollListTimer();
  }
  
  private void cancelRollListTimer() {
    if (rollListTimer != null) {
      rollListTimer.cancel();
      rollListTimer = null;
    }
  }
  /**
   * A call to this function peeks elements from the first local primary bucket. 
   * Next call to this function peeks elements from the next local primary 
   * bucket and so on.  
   */
  @Override
  public List peek(int batchSize, int timeToWait) throws InterruptedException,
  CacheException {
    
    List batch = new ArrayList();
    
    int batchSizeInBytes = batchSize*1024*1024;
    PartitionedRegion prQ = getRandomShadowPR();
    if (prQ == null || prQ.getLocalMaxMemory() == 0) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      blockProcesorThreadIfRequired();
      return batch;
    }
    
    ArrayList list = null;
    ArrayList<Integer> pbuckets = new ArrayList<Integer>(prQ
        .getDataStore().getAllLocalPrimaryBucketIds());
    ArrayList<Integer> buckets = new ArrayList<Integer>();
    for(Integer i : pbuckets) {
    	if(i % this.nDispatcher == this.index)
    		buckets.add(i);
    }
    // In case of failures, peekedEvents would possibly have some elements 
    // add them. 
    if (this.resetLastPeeked) {
      int previousBucketId = -1;
      boolean stillPrimary = true; 
      Iterator<GatewaySenderEventImpl>  iter = peekedEvents.iterator();
      // we need to remove the events of the bucket that are no more primary on 
      // this node as they cannot be persisted from this node. 
      while(iter.hasNext()) {
        HDFSGatewayEventImpl hdfsEvent = (HDFSGatewayEventImpl)iter.next();
        if (previousBucketId != hdfsEvent.getBucketId()){
          stillPrimary = buckets.contains(hdfsEvent.getBucketId());
          previousBucketId = hdfsEvent.getBucketId();
        }
        if (stillPrimary)
          batch.add(hdfsEvent);
        else {
          iter.remove();
        }
      }
      this.resetLastPeeked = false;
    }
    
    if (buckets.size() == 0) {
      // Sleep a bit before trying again. provided by Dan
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return batch;
    }
    
    if (this.sender.getBucketSorted()) {
      
    }
    
    // Each call to this function returns index of next bucket 
    // that is to be processed. This function takes care 
    // of the bucket sequence that is peeked by a sequence of 
    // peek calls. 
    // If there are bucket movements between two consecutive 
    // calls to this function then there is chance that a bucket 
    // is processed twice while another one is skipped. But, that is 
    // ok because in the next round, it will be processed. 
    Integer bIdIndex = getCurrentBucketIndex(buckets.size());
    
    // If we have gone through all the buckets once and no  
    // elements were peeked from any of the buckets, take a nap.  
    // This always sleep in the first call but that should be ok  
    // because the timeToWait in practical use cases would be greater 
    // than this sleep of 100 ms.  
    if (bIdIndex == 0 && getAndresetElementsPeekedAcrossBuckets() == 0) { 
      try { 
        Thread.sleep(100); 
      } catch (InterruptedException e) { 
        Thread.currentThread().interrupt(); 
      } 
    } 
    
    HDFSBucketRegionQueue hrq = ((HDFSBucketRegionQueue)prQ
        .getDataStore().getLocalBucketById(buckets.get(bIdIndex)));
    
    if (hrq == null) {
      // bucket moved to another node after getAllLocalPrimaryBucketIds
      // was called. Peeking not possible. return. 
      return batch;
    }
    long entriesWaitingTobePeeked = hrq.totalEntries();
    
    if (entriesWaitingTobePeeked == 0) {
      blockProcesorThreadIfRequired();
      return batch;
    }
    
    long currentTimeInMillis = System.currentTimeMillis();
    long bucketSizeInBytes = hrq.getQueueSizeInBytes();
    if (((currentTimeInMillis - hrq.getLastPeekTimeInMillis()) >  timeToWait)  
        || ( bucketSizeInBytes > batchSizeInBytes)
        || hrq.shouldDrainImmediately()) {
      // peek now
      if (logger.isDebugEnabled()) { 
        logger.debug("Peeking queue " + hrq.getId()   + ": bucketSizeInBytes " + bucketSizeInBytes
            + ":  batchSizeInBytes" + batchSizeInBytes
            + ":  timeToWait" + timeToWait
            + ":  (currentTimeInMillis - hrq.getLastPeekTimeInMillis())" + (currentTimeInMillis - hrq.getLastPeekTimeInMillis()));
      }

      list = peekAhead(buckets.get(bIdIndex), hrq);
      
      if (list != null && list.size() != 0 ) {
        for (Object object : list) {
          batch.add(object);
          peekedEvents.add((HDFSGatewayEventImpl)object);
        }
      }
    }
    else {
      blockProcesorThreadIfRequired();
    }
    if (logger.isDebugEnabled()  &&  batch.size() > 0) {
      logger.debug(this + ":  Peeked a batch of " + batch.size() + " entries");
    }
    
    setElementsPeekedAcrossBuckets(batch.size()); 
    
    return batch;
  }
  
  /**
   * This function maintains an index of the last processed bucket.
   * When it is called, it returns index of the next bucket. 
   * @param totalBuckets
   * @return current bucket index
   */
  private int getCurrentBucketIndex(int totalBuckets) {
    int retBucket = currentBucketIndex;
    if (retBucket >=  totalBuckets) {
      currentBucketIndex = 0;
      retBucket = 0;
    }
    
    currentBucketIndex++;
    
    return retBucket;
  }
  
  @Override
  public void remove(int batchSize) throws CacheException {
    int destroyed = 0;
    HDFSGatewayEventImpl event = null;
    
    if (this.peekedEvents.size() > 0)
      event = (HDFSGatewayEventImpl)this.peekedEvents.remove();
    
    while (event != null && destroyed < batchSize) {
      Region currentRegion = event.getRegion();
      int currentBucketId = event.getBucketId();
      int bucketId = event.getBucketId();
        
      ArrayList<HDFSGatewayEventImpl> listToDestroy = new ArrayList<HDFSGatewayEventImpl>();
      ArrayList<Object> destroyedSeqNum = new ArrayList<Object>();
      
      // create a batch of all the entries of a bucket 
      while (bucketId == currentBucketId) {
        listToDestroy.add(event);
        destroyedSeqNum.add(event.getShadowKey());
        destroyed++;

        if (this.peekedEvents.size() == 0 || (destroyed) >= batchSize) {
          event = null; 
          break;
        }

        event = (HDFSGatewayEventImpl)this.peekedEvents.remove();

        bucketId = event.getBucketId();

        if (!this.sender.isRunning()){
          if (logger.isDebugEnabled()) {
            logger.debug("ParallelGatewaySenderQueue#remove: Cache is closing down. Ignoring remove request.");
          }
          return;
        }
      }
      try {
        HDFSBucketRegionQueue brq = getBucketRegionQueue((PartitionedRegion) currentRegion, currentBucketId);
        
        if (brq != null) {
          // destroy the entries from the bucket 
          brq.destroyKeys(listToDestroy);
          // Adding the removed event to the map for BatchRemovalMessage
          // We need to provide the prQ as there could be multiple
          // queue in a PGS now.
          PartitionedRegion prQ = brq.getPartitionedRegion();
          addRemovedEvents(prQ, currentBucketId, destroyedSeqNum);
        }
        
      } catch (ForceReattemptException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("ParallelGatewaySenderQueue#remove: " + "Got ForceReattemptException for " + this
          + " for bucket = " + bucketId);
        }
      }
      catch(EntryNotFoundException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("ParallelGatewaySenderQueue#remove: " + "Got EntryNotFoundException for " + this
            + " for bucket = " + bucketId );
        }
      }
    }
  }
  
  /** 
  * Keeps a track of number of elements peeked across all buckets.  
  */ 
  private void setElementsPeekedAcrossBuckets(int peekedElements) { 
    this.elementsPeekedAcrossBuckets +=peekedElements; 
  } 
  
  /** 
  * Returns the number of elements peeked across all buckets. Also, 
  * resets this counter. 
  */ 
  private int getAndresetElementsPeekedAcrossBuckets() { 
    int peekedElements = this.elementsPeekedAcrossBuckets; 
    this.elementsPeekedAcrossBuckets = 0; 
    return peekedElements; 
  } 

  @Override
  public void remove() throws CacheException {
    throw new UnsupportedOperationException("Method HDFSParallelGatewaySenderQueue#remove is not supported");
  }
 
  @Override
  public void put(Object object) throws InterruptedException, CacheException {
    super.put(object);
  }
  
  protected ArrayList peekAhead(int bucketId, HDFSBucketRegionQueue hrq) throws CacheException {
    
    if (logger.isDebugEnabled()) {
      logger.debug(this + ": Peekahead for the bucket " + bucketId);
    }
    ArrayList  list = hrq.peekABatch();
    if (logger.isDebugEnabled() && list != null ) {
      logger.debug(this + ": Peeked" + list.size() + "objects from bucket " + bucketId);
    }

    return list;
  }
  
  @Override
  public Object take() {
    throw new UnsupportedOperationException("take() is not supported for " + HDFSParallelGatewaySenderQueue.class.toString());
  }
  
  protected boolean isUsedForHDFS()
  {
    return true;
  }
  
  @Override
  protected void afterRegionAdd (PartitionedRegion userPR) {
  }
  
  /**
   * gets the value for region key from the HDFSBucketRegionQueue 
 * @param region 
   * @throws ForceReattemptException 
   */
  public HDFSGatewayEventImpl get(PartitionedRegion region, byte[] regionKey, int bucketId) throws ForceReattemptException  {
    try {
      HDFSBucketRegionQueue brq = getBucketRegionQueue(region, bucketId);
      
      if (brq ==null)
        return null;
      
      return brq.getObjectForRegionKey(region, regionKey);
    } catch(EntryNotFoundException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("HDFSParallelGatewaySenderQueue#get: " + "Got EntryNotFoundException for " + this
            + " for bucket = " + bucketId);
      }
    }
    return null;
  }

  @Override
  public void clear(PartitionedRegion pr, int bucketId) {
    HDFSBucketRegionQueue brq;
    try {
      brq = getBucketRegionQueue(pr, bucketId);
      if (brq == null)
        return;
      brq.clear();
    } catch (ForceReattemptException e) {
      //do nothing, bucket was destroyed.
    }
  }
  
  @Override
  public int size(PartitionedRegion pr, int bucketId) throws ForceReattemptException {
   HDFSBucketRegionQueue hq = getBucketRegionQueue(pr, bucketId);
   return hq.size();
  }

  public HDFSBucketRegionQueue getBucketRegionQueue(PartitionedRegion region,
      int bucketId) throws ForceReattemptException {
    PartitionedRegion leader = ColocationHelper.getLeaderRegion(region);
    if (leader == null)
      return null;
    String leaderregionPath = leader.getFullPath();
    PartitionedRegion prQ = this.userRegionNameToshadowPRMap.get(leaderregionPath);
    if (prQ == null)
      return null;
    HDFSBucketRegionQueue brq;

    brq = ((HDFSBucketRegionQueue)prQ.getDataStore()
        .getLocalBucketById(bucketId));
    if(brq == null) {
      prQ.getRegionAdvisor().waitForLocalBucketStorage(bucketId);
    }
    brq = ((HDFSBucketRegionQueue)prQ.getDataStore()
        .getInitializedBucketForId(null, bucketId));
    return brq;
  }
  
  /**
   * This class has the responsibility of rolling the lists of Sorted event 
   * Queue. The rolling of lists by a separate thread is required because 
   * neither put thread nor the peek/remove thread can do that. Put thread
   * cannot do it because that would mean doing some synchronization with 
   * other put threads and peek thread that would hamper the put latency. 
   * Peek thread cannot do it because if the event insert rate is too high
   * the list size can go way beyond what its size. 
   * @author hemantb
   *
   */
  class RollSortedListsTimerTask extends SystemTimerTask {
    
    
    /**
     * This function ensures that if any of the buckets has lists that are beyond 
     * its size, they gets rolled over into new skip lists. 
     */
    @Override
    public void run2() {
      Set<PartitionedRegion> prQs = getRegions();
      for (PartitionedRegion prQ : prQs) {
        ArrayList<Integer> buckets = new ArrayList<Integer>(prQ
            .getDataStore().getAllLocalPrimaryBucketIds());
        for (Integer bId : buckets) {
          HDFSBucketRegionQueue hrq =  ((HDFSBucketRegionQueue)prQ
              .getDataStore().getLocalBucketById(bId));
          if (hrq == null) {
            // bucket moved to another node after getAllLocalPrimaryBucketIds
            // was called. continue fixing the next bucket. 
            continue;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Rolling over the list for bucket id: " + bId);
          }
          hrq.rolloverSkipList();
         }
      }
    }
  }
   
}
