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


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.hdfs.internal.FlushObserver.AsyncFlushResult;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue.SortedEventBuffer.BufferIterator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.AbstractBucketRegionQueue;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.cache.persistence.soplog.ByteComparator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.CursorIterator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * This class holds the sorted list required for HDFS. 
 * 
 * @author Hemant Bhanawat
 * 
 */
public class HDFSBucketRegionQueue extends AbstractBucketRegionQueue {
     private static final boolean VERBOSE = Boolean.getBoolean("hdfsBucketRegionQueue.VERBOSE");
     private final int batchSize;
     volatile HDFSEventQueue hdfsEventQueue = null;
     
     // set before releasing the primary lock. 
     private final AtomicBoolean releasingPrimaryLock = new AtomicBoolean(true);
     
     // This is used to keep track of the current size of the queue in bytes. 
     final AtomicLong queueSizeInBytes =  new AtomicLong(0);
     public boolean isBucketSorted = true;
     /**
     * @param regionName
     * @param attrs
     * @param parentRegion
     * @param cache
     * @param internalRegionArgs
     */
    public HDFSBucketRegionQueue(String regionName, RegionAttributes attrs,
        LocalRegion parentRegion, GemFireCacheImpl cache,
        InternalRegionArguments internalRegionArgs) {
      super(regionName, attrs, parentRegion, cache, internalRegionArgs);
      
      this.isBucketSorted = internalRegionArgs.getPartitionedRegion().getParallelGatewaySender().getBucketSorted();
      if (isBucketSorted)
        hdfsEventQueue = new MultiRegionSortedQueue();
      else
        hdfsEventQueue = new EventQueue();
      
      batchSize = internalRegionArgs.getPartitionedRegion().
          getParallelGatewaySender().getBatchSize() * 1024 *1024;
      this.keySet();
    }
    @Override
    protected void initialize(InputStream snapshotInputStream,
        InternalDistributedMember imageTarget,
        InternalRegionArguments internalRegionArgs) throws TimeoutException,
        IOException, ClassNotFoundException {

      super.initialize(snapshotInputStream, imageTarget, internalRegionArgs);

      loadEventsFromTempQueue();
      
      this.initialized = true;
      notifyEventProcessor();
    }

    private TreeSet<Long> createSkipListFromMap(Set keySet) {
      TreeSet<Long> sortedKeys = null;
      if (!hdfsEventQueue.isEmpty())
        return sortedKeys;
      
      if (!keySet.isEmpty()) {
        sortedKeys = new TreeSet<Long>(keySet);
        if (!sortedKeys.isEmpty())
        {
          for (Long key : sortedKeys) {
            if (this.isBucketSorted) {
              Object hdfsevent = getNoLRU(key, true, false, false);
              if (hdfsevent == null) { // this can happen when tombstones are recovered. 
                if (logger.isDebugEnabled() || VERBOSE) {
                  logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Discarding key " + key + ", no event recovered"));
                }
              } else {
                int eventSize = ((HDFSGatewayEventImpl)hdfsevent).
                    getSizeOnHDFSInBytes(!this.isBucketSorted);
                hdfsEventQueue.put(key,(HDFSGatewayEventImpl)hdfsevent, eventSize );
                queueSizeInBytes.getAndAdd(eventSize);
              }
            }
            else {
              Object hdfsevent = getNoLRU(key, true, false, false);
              if (hdfsevent != null) { // hdfs event can be null when tombstones are recovered.
                queueSizeInBytes.getAndAdd(((HDFSGatewayEventImpl)hdfsevent).
                    getSizeOnHDFSInBytes(!this.isBucketSorted));
              }
              ((EventQueue)hdfsEventQueue).put(key);
            }
              
          }
          getEventSeqNum().setIfGreater(sortedKeys.last());
        }
      
      }
      if (logger.isDebugEnabled() || VERBOSE) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG,
            "For bucket " + getId() + ", total keys recovered are : " + keySet.size()
                + " and the seqNo is " + getEventSeqNum()));
      }
      return sortedKeys;
    }
    
    @Override
    protected void basicClear(RegionEventImpl ev) {
      super.basicClear(ev);
      queueSizeInBytes.set(0);
      if ( this.getBucketAdvisor().isPrimary()) {
        this.hdfsEventQueue.clear();
      }
    }
    
    protected void clearQueues(){
      queueSizeInBytes.set(0);
      if ( this.getBucketAdvisor().isPrimary()) {
        this.hdfsEventQueue.clear();
      }
    }
   
    @Override
    protected void basicDestroy(final EntryEventImpl event,
        final boolean cacheWrite, Object expectedOldValue)
        throws EntryNotFoundException, CacheWriterException, TimeoutException {
      super.basicDestroy(event, cacheWrite, expectedOldValue);
    }
    
    ArrayList peekABatch() {
      ArrayList result = new ArrayList();
      hdfsEventQueue.peek(result);
      return result;
    }
    
    @Override
    protected void addToEventQueue(Object key, boolean didPut, EntryEventImpl event, int sizeOfHDFSEvent) {
      if (didPut &&  this.getBucketAdvisor().isPrimary()) {
        HDFSGatewayEventImpl hdfsEvent = (HDFSGatewayEventImpl)event.getValue();
        if (sizeOfHDFSEvent == -1) { 
          try {
            // the size is calculated only on primary before event is inserted in the bucket. 
            // If this node became primary after size was calculated, sizeOfHDFSEvent will be -1. 
            // Try to get the size. #50016
            sizeOfHDFSEvent = hdfsEvent.getSizeOnHDFSInBytes(!((HDFSBucketRegionQueue)this).isBucketSorted);
          } catch (Throwable e) {
           //   Ignore any exception while fetching the size.
            sizeOfHDFSEvent = 0;
          }
        }
        queueSizeInBytes.getAndAdd(sizeOfHDFSEvent);
        if (this.initialized) {
          Long longKey = (Long)key;
          this.hdfsEventQueue.put(longKey, hdfsEvent, sizeOfHDFSEvent);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Put successfully in the queue : " + hdfsEvent + " . Queue initialized: " 
              + this.initialized);
        }
      }
    }
    
    /**
     * It removes the first key from the queue.
     * 
     * @return Returns the key for which value was destroyed.
     * @throws ForceReattemptException
     */
    public Long remove() throws ForceReattemptException {
      throw new UnsupportedOperationException("Individual entries cannot be removed in a HDFSBucketRegionQueue");
    }
    
    /**
     * It removes the first key from the queue.
     * 
     * @return Returns the value.
     * @throws InterruptedException
     * @throws ForceReattemptException
     */
    public Object take() throws InterruptedException, ForceReattemptException {
      throw new UnsupportedOperationException("take() cannot be called for individual entries in a HDFSBucketRegionQueue");
    }
    
    public void destroyKeys(ArrayList<HDFSGatewayEventImpl>  listToDestroy) {
      
      HashSet<Long> removedSeqNums = new HashSet<Long>();
      
      for (int index =0; index < listToDestroy.size(); index++) {
        HDFSGatewayEventImpl entry = null;
        if (this.isBucketSorted) {
          // Remove the events in reverse order so that the events with higher sequence number
          // are removed last to ensure consistency.
          entry = listToDestroy.get(listToDestroy.size() - index -1);
        } else {
          entry = listToDestroy.get(index);
        }
       
        try {
          if (this.logger.isDebugEnabled())
            logger.debug("destroying primary key " + entry.getShadowKey() + " bucket id: " + this.getId());
          // removed from peeked list
          boolean deleted = this.hdfsEventQueue.remove(entry);
          if (deleted) {
            // this is an onheap event so a call to size should be ok. 
            long entrySize = entry.getSizeOnHDFSInBytes(!this.isBucketSorted);
            destroyKey(entry.getShadowKey());
            long queueSize = queueSizeInBytes.getAndAdd(-1*entrySize);
            if (queueSize < 0) {
              // In HA scenarios, queueSizeInBytes can go awry.
              queueSizeInBytes.compareAndSet(queueSize, 0);
            }
            removedSeqNums.add(entry.getShadowKey());
          }
        }catch (ForceReattemptException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("ParallelGatewaySenderQueue#remove->HDFSBucketRegionQueue#destroyKeys: " + "Got ForceReattemptException for " + this
            + " for bucket = " + this.getId());
          }
        }
        catch(EntryNotFoundException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("ParallelGatewaySenderQueue#remove->HDFSBucketRegionQueue#destroyKeys: " + "Got EntryNotFoundException for " + this
              + " for bucket = " + this.getId() + " and key " + entry.getShadowKey());
          }
        } finally {
          entry.release();
        }
      }
      
      if (this.getBucketAdvisor().isPrimary()) {
        hdfsEventQueue.handleRemainingElements(removedSeqNums);
      }
    }

    
    public boolean isReadyForPeek() {
      return !this.isEmpty() && !this.hdfsEventQueue.isEmpty() && getBucketAdvisor().isPrimary();
    }

    public long getLastPeekTimeInMillis() {
      return hdfsEventQueue.getLastPeekTimeInMillis();
    }
    
    public long getQueueSizeInBytes() {
      return queueSizeInBytes.get();
    }
    /*
     * This function is called when the bucket takes as the role of primary.
     */
    @Override
    public void beforeAcquiringPrimaryState() {
      
      queueSizeInBytes.set(0);
      if (logger.isDebugEnabled() || VERBOSE) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG,
            "This node has become primary for bucket " + this.getId()  +". " +
            		"Creating sorted data structure for the async queue."));
      }
      releasingPrimaryLock.set(false);
      
      // clear the hdfs queue in case it has already elements left if it was a primary
      // in the past
      hdfsEventQueue.clear();
      if (isBucketSorted)
        hdfsEventQueue = new MultiRegionSortedQueue();
      else
        hdfsEventQueue = new EventQueue();
      
      TreeSet<Long> sortedKeys = createSkipListFromMap(this.keySet());
      
      if (sortedKeys != null && sortedKeys.size() > 0) {    
        // Mark the events equal to batch size as duplicate. 
        // calculate the batch size based on the number of events currently in the queue
        // This is an approximation. 
        long batchSizeMB =  this.getPartitionedRegion().getParallelGatewaySender().getBatchSize();
        long batchSizeInBytes = batchSizeMB*1024*1024;
        long totalBucketSize = queueSizeInBytes.get();
        totalBucketSize = totalBucketSize >  0 ? totalBucketSize: 1;
        long totalEntriesInBucket = this.entryCount();
        totalEntriesInBucket =  totalEntriesInBucket > 0 ? totalEntriesInBucket: 1;
        
        long perEntryApproxSize = totalBucketSize/totalEntriesInBucket;
        perEntryApproxSize = perEntryApproxSize >  0 ? perEntryApproxSize: 1;
        
        int batchSize  = (int)(batchSizeInBytes/perEntryApproxSize);
        
        if (logger.isDebugEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG,
              "Calculating batch size " +  " batchSizeMB: " + batchSizeMB + " batchSizeInBytes: " + batchSizeInBytes + 
              " totalBucketSize: " + totalBucketSize + " totalEntriesInBucket: " + totalEntriesInBucket + 
              " perEntryApproxSize: " + perEntryApproxSize + " batchSize: " + batchSize ));
        }
        
        markEventsAsDuplicate(batchSize, sortedKeys.iterator());
      }
    }
    
    @Override
    public void beforeReleasingPrimaryLockDuringDemotion() {
      queueSizeInBytes.set(0);
      releasingPrimaryLock.set(true);
      // release memory in case of a clean transition
      hdfsEventQueue.clear();
    }

    /**
     * This function searches the skip list and the peeked skip list for a given region key
     * @param region 
     * 
     */
    public HDFSGatewayEventImpl getObjectForRegionKey(Region region, byte[] regionKey) {
      // get can only be called for a sorted queue.
      // Calling get with Long.MIN_VALUE seq number ensures that 
      // the list will return the key which has highest seq number. 
      return hdfsEventQueue.get(region, regionKey, Long.MIN_VALUE);
    }

    /**
     * Get an iterator on the queue, passing in the partitioned region
     * we want to iterate over the events from.
     */
    public SortedEventQueueIterator iterator(Region region) {
      return hdfsEventQueue.iterator(region);
    }

    public long totalEntries() {
      return entryCount();
    }
    
    /**
     * Ideally this function should be called from a thread periodically to 
     * rollover the skip list when it is above a certain size. 
     * 
     */
    public void rolloverSkipList() {
      // rollover can only be called for a sorted queue.
      hdfsEventQueue.rollover();
    }
    
    public boolean shouldDrainImmediately() {
      return hdfsEventQueue.getFlushObserver().shouldDrainImmediately();
    }

    public AsyncFlushResult flush() {
      if (logger.isDebugEnabled() || VERBOSE) {
        logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Flush requested"));
      }
      return hdfsEventQueue.getFlushObserver().flush();
    }
    
    /**
     * This class keeps the regionkey and seqNum. The objects of this class are 
     * kept in a concurrent skip list. The order of elements is decided based on the 
     * comparison of regionKey + seqNum. This kind of comparison allows us to keep 
     * multiple updates on a single key (becaus it has different seq Num)
     */
    static class KeyToSeqNumObject implements Comparable<KeyToSeqNumObject>
    {
      private byte[] regionkey; 
      private Long seqNum;
      
      KeyToSeqNumObject(byte[] regionkey, Long seqNum){
        this.regionkey = regionkey;
        this.seqNum = seqNum;
      }
      
      /**
       * This function compares the key first. If the keys are same then seq num is compared.
       * This function is a key function because it ensures that the skiplists hold the elements 
       * in an order we want it to and for multiple updates on key fetches the most recent one 
       * Currently we are comparing seq numbers but we will have to change it to version stamps. 
       * * List can have elements in following sequence 
       * K1 Value1 version : 1 
       * K2 Value2a version : 2
       * K2 Value2 version : 1
       * K3 Value3 version : 1
       * For a get on K2, it should retunr K2 Value 2a.  
       */
      @Override
      public int compareTo(KeyToSeqNumObject o) {
        int compareOutput = ByteComparator.compareBytes(
            this.getRegionkey(), 0, this.getRegionkey().length, o.getRegionkey(), 0, o.getRegionkey().length);
        if (compareOutput != 0 )
          return compareOutput;
        
        // If the keys are same and this is an object with dummy seq number, 
        // return -1. This will ensure that ceiling function on a skip list will enumerate 
        // all the entries and return the last one.   
        if (this.getSeqNum() == Long.MIN_VALUE) 
          return -1;
        
        // this is to just maintain consistency with the above statement. 
        if (o.getSeqNum() == Long.MIN_VALUE) 
          return 1;
       
        // minus operator pushes entries with lower seq number in the end so that 
        // the order as mentioned above is maintained. And the entries with 
        // higher version are fetched on a get. 
        return this.getSeqNum().compareTo(o.getSeqNum()) * -1;  
      }
      
      @Override
      public boolean equals (Object o) {
    	KeyToSeqNumObject obj = null;
      	if (o == null)
    		return false; 
    	
    	if (o instanceof KeyToSeqNumObject) 
    		obj = (KeyToSeqNumObject)o;
    	else
    		return false;
    	
    	if (this.compareTo(obj) != 0)
          return false;
        else
          return true;
      }
      
      public int hashCode() {
    	assert false : "hashCode not designed";
    	return -1;
      }
      
      byte[] getRegionkey() {
        return regionkey;
      }

      public Long getSeqNum() {
        return seqNum;
      }

      public void setSeqNum(Long seqNum) {
        this.seqNum = seqNum;
      }
      
      @Override
      public String toString() {
        return EntryEventImpl.deserialize(regionkey) + " {" + seqNum + "}";
      }
    }
    
    public interface HDFSEventQueue {
      FlushObserver getFlushObserver();
      
      /** puts an event in the queue. */ 
      public void put (long key, HDFSGatewayEventImpl event, int size);
      
      public SortedEventQueueIterator iterator(Region region);

      public void rollover();

      /** Get a value from the queue
       * @throws IllegalStateException if this queue doesn't support get  
       **/
      public HDFSGatewayEventImpl get(Region region, byte[] regionKey,
          long minValue);

      // Peeks a batch of size specified by batchSize
      // And add the results to the array list
      public void peek(ArrayList result);
      
      // Checks if there are elements to bee peeked 
      public boolean isEmpty();
      
      // removes the event if it has already been peeked. 
      public boolean remove(HDFSGatewayEventImpl event);
      
      // take care of the elements that were peeked 
      // but were not removed after a batch dispatch 
      // due to concurrency effects. 
      public void handleRemainingElements(HashSet<Long> listToBeremoved);
      
      // clears the list. 
      public void clear();
      
      // get the time when the last peek was done. 
      public long getLastPeekTimeInMillis();
    }
    
    class MultiRegionSortedQueue implements HDFSEventQueue {
      ConcurrentMap<String, SortedEventQueue> regionToEventQueue = new ConcurrentHashMap<String, SortedEventQueue>();
      volatile Set<SortedEventQueue> peekedQueues = Collections.EMPTY_SET;
      private final AtomicBoolean peeking = new AtomicBoolean(false);
      long lastPeekTimeInMillis = System.currentTimeMillis();
      
      private final FlushObserver flush = new FlushObserver() {
        @Override
        public AsyncFlushResult flush() {
          final Set<AsyncFlushResult> flushes = new HashSet<AsyncFlushResult>();
          for (SortedEventQueue queue : regionToEventQueue.values()) {
            flushes.add(queue.getFlushObserver().flush());
          }
          
          return new AsyncFlushResult() {
            @Override
            public boolean waitForFlush(long timeout, TimeUnit unit) throws InterruptedException {
              long start = System.nanoTime();
              long remaining = unit.toNanos(timeout);
              for (AsyncFlushResult afr : flushes) {
                if (!afr.waitForFlush(remaining, TimeUnit.NANOSECONDS)) {
                  return false;
                }
                remaining -= (System.nanoTime() - start);
              }
              return true;
            }
          };
        }
        
        @Override
        public boolean shouldDrainImmediately() {
          for (SortedEventQueue queue : regionToEventQueue.values()) {
            if (queue.getFlushObserver().shouldDrainImmediately()) {
              return true;
            }
          }
          return false;
        }
      };
      
      @Override
      public FlushObserver getFlushObserver() {
        return flush;
      }

      @Override
      public void put(long key, HDFSGatewayEventImpl event, int size) {
        
        String region = event.getRegionPath();
        SortedEventQueue regionQueue = regionToEventQueue.get(region);
        if(regionQueue == null) {
          regionToEventQueue.putIfAbsent(region, new SortedEventQueue());
          regionQueue = regionToEventQueue.get(region);
        }
        regionQueue.put(key, event, size);
      }

      @Override
      public void peek(ArrayList result) {
        // The elements that were peeked last time, have not been persisted to HDFS 
        // yet. You cannot take out next batch until that is done.
        if (!peeking.compareAndSet(false, true)) {
          if (logger.isTraceEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Peek already in progress, aborting"));
          }
          return;
        }
        //Maintain a separate set of peeked queues.
        //All of these queues are statefull, and expect to be
        //handleRemainingElements and clear to be called on
        //them iff peek was called on them. However, new queues
        //may be created in that time.
        peekedQueues = Collections.newSetFromMap(new ConcurrentHashMap<SortedEventQueue, Boolean>(regionToEventQueue.size()));
        
        //Peek from all of the existing queues
        for(SortedEventQueue queue : regionToEventQueue.values()) {
          if(!queue.isEmpty()) {
            queue.peek(result);
            peekedQueues.add(queue);
          }
        }
        if (result.isEmpty()) 
          peeking.set(false);
        
        
        this.lastPeekTimeInMillis = System.currentTimeMillis();
      }

      @Override
      public boolean isEmpty() {
        for(SortedEventQueue queue : regionToEventQueue.values()) {
          if(!queue.isEmpty()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean remove(HDFSGatewayEventImpl event) {
        String region = event.getRegionPath();
        SortedEventQueue regionQueue = regionToEventQueue.get(region);
        return regionQueue.remove(event);
      }

      @Override
      public void handleRemainingElements(HashSet<Long> removedSeqNums){
        for(SortedEventQueue queue : peekedQueues) {
          queue.handleRemainingElements(removedSeqNums);
        }
        peekedQueues.clear();
        peeking.set(false);
      }

      @Override
      public void clear() {
        for(SortedEventQueue queue : regionToEventQueue.values()) {
          queue.clear();
        }
        peekedQueues.clear();
        peeking.set(false);
      }

      @Override
      public long getLastPeekTimeInMillis() {
        return this.lastPeekTimeInMillis;
      }

      @Override
      public HDFSGatewayEventImpl get(Region region, byte[] regionKey,
          long minValue) {
        SortedEventQueue queue = regionToEventQueue.get(region.getFullPath());
        if(queue == null) {
          return null;
        }
        return queue.get(region, regionKey, minValue);
      }

      @Override
      public SortedEventQueueIterator iterator(Region region) {
        SortedEventQueue queue = regionToEventQueue.get(region.getFullPath());
        if(queue == null) {
          return new SortedEventQueueIterator(new LinkedBlockingDeque<SortedEventBuffer>());
        }
        return queue.iterator(region);
      }

      @Override
      public void rollover() {
        for(SortedEventQueue queue : regionToEventQueue.values()) {
          queue.rollover();
        }
      }
    }
    
    class EventQueue implements HDFSEventQueue {
      private final SignalledFlushObserver flush = new SignalledFlushObserver();
      private final BlockingQueue<Long> eventSeqNumQueue = new LinkedBlockingQueue<Long>();
      private final BlockingQueue<Long> peekedEvents = new LinkedBlockingQueue<Long>();
      private long lastPeekTimeInMillis = System.currentTimeMillis(); 
      
      public EventQueue() {
        
      }
      
      @Override
      public FlushObserver getFlushObserver() {
        return flush;
      }

      @Override
      public void put(long key, HDFSGatewayEventImpl event, int size) {
        put(key);
      }
      public void put (long key) {
        eventSeqNumQueue.add(key);
        flush.push();
        incQueueSize();
      }
      
      
      @Override
      public HDFSGatewayEventImpl get(Region region, byte[] regionKey,
          long minValue) {
        throw new InternalGemFireError("Get not supported on unsorted queue");
      }
      
      @Override
      public void peek(ArrayList peekedEntries) {
        if (peekedEvents.size() != 0) {
          return;
        }
        
        for(int size=0; size < batchSize; ) {
          Long seqNum = eventSeqNumQueue.peek();
          if (seqNum == null) {
            // queue is now empty, return
            break;
          }
          Object object = getNoLRU(seqNum, true, false, false);
          if (object != null) {
            peekedEvents.add(seqNum);
            size += ((HDFSGatewayEventImpl)object).getSizeOnHDFSInBytes(!isBucketSorted);
            peekedEntries.add(object);

          } else {
            logger.debug("The entry corresponding to the sequence number " + 
               seqNum +  " is missing. This can happen when an entry is already" +
               "dispatched before a bucket moved.");
            // event is being ignored. Decrease the queue size
            decQueueSize();
            flush.pop(1);
           
          }
          eventSeqNumQueue.poll();
          
        }
        this.lastPeekTimeInMillis  = System.currentTimeMillis();
      }

      @Override
      public boolean isEmpty() {
        return eventSeqNumQueue.isEmpty();
      }

      
      @Override
      public boolean remove(HDFSGatewayEventImpl event) {
        boolean deleted = peekedEvents.remove(event.getShadowKey());
        if (deleted)
         decQueueSize();
        return deleted;
      }

      @Override
      // It looks like that there is no need for this function 
      // in EventQueue.
      public void handleRemainingElements(HashSet<Long> removedSeqNums) {
        flush.pop(removedSeqNums.size());
        eventSeqNumQueue.addAll(peekedEvents);
        peekedEvents.clear();
      }

      @Override
      public void clear() {
        flush.clear();
        decQueueSize(eventSeqNumQueue.size());
        eventSeqNumQueue.clear();
        decQueueSize(peekedEvents.size());
        peekedEvents.clear();
      }

      @Override
      public long getLastPeekTimeInMillis() {
        return this.lastPeekTimeInMillis;
      }
      @Override
      public SortedEventQueueIterator iterator(Region region) {
        throw new InternalGemFireError("not supported on unsorted queue");
      }
      @Override
      public void rollover() {
        throw new InternalGemFireError("not supported on unsorted queue");
      }
    }
    
    class SortedEventQueue implements HDFSEventQueue {
      private final SignalledFlushObserver flush = new SignalledFlushObserver();

      // List of all the skip lists that hold the data
      final Deque<SortedEventBuffer> queueOfLists = 
          new LinkedBlockingDeque<SortedEventBuffer>();
      
      // This points to the tail of the queue
      volatile SortedEventBuffer currentSkipList = new SortedEventBuffer();
      
      private final AtomicBoolean peeking = new AtomicBoolean(false);
      
      private long lastPeekTimeInMillis = System.currentTimeMillis(); 
      
      public SortedEventQueue() {
        queueOfLists.add(currentSkipList);
      }
      
      @Override
      public FlushObserver getFlushObserver() {
        return flush;
      }

      public boolean remove(HDFSGatewayEventImpl event) {
        SortedEventBuffer eventBuffer = queueOfLists.peek();
        if (eventBuffer != null) {
          return eventBuffer.copyToBuffer(event);
        }
        else {
          // This can happen when the queue is cleared because of bucket movement 
          // before the remove is called. 
          return true;
        }
      } 

      public void clear() {
        flush.clear();
        for (SortedEventBuffer buf : queueOfLists) {
          decQueueSize(buf.size());
          buf.clear();
        }
        
        queueOfLists.clear();
        rollList(false);

        peeking.set(false);
      }

      public boolean isEmpty() {
        if (queueOfLists.size() == 1)
          return queueOfLists.peek().isEmpty();
        return false;
      }

      public void put(long key, HDFSGatewayEventImpl event, int eventSize) {
        if (logger.isTraceEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Inserting key " + event + " into list " + System.identityHashCode(currentSkipList)));
        }
        putInList(new KeyToSeqNumObject(((HDFSGatewayEventImpl)event).getSerializedKey(), key), 
            eventSize);
      }

      private void putInList(KeyToSeqNumObject entry, int sizeInBytes) {
        // It was observed during testing that peek can start peeking 
        // elements from a list to which a put is happening. This happens 
        // when the peek changes the value of currentSkiplist to a new list 
        // but the put continues to write to an older list. 
        // So there is a possibility that an element is added to the list 
        // that has already been peeked. To handle this case, in handleRemainingElements
        // function we re-add the elements that were not peeked. 
        if (currentSkipList.add(entry, sizeInBytes) == null) {
          flush.push();
          incQueueSize();
        }
      }

      public void rollover(boolean forceRollover) {
        if (currentSkipList.bufferSize() >= batchSize || forceRollover) {
          rollList(forceRollover);
        }
      }
      
      /**
       * Ideally this function should be called from a thread periodically to 
       * rollover the skip list when it is above a certain size. 
       * 
       */
      public void rollover() {
        rollover(false);
      }

      public void peek(ArrayList peekedEntries) {
        // The elements that were peeked last time, have not been persisted to HDFS 
        // yet. You cannot take out next batch until that is done.
        if (!peeking.compareAndSet(false, true)) {
          if (logger.isTraceEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Peek already in progress, aborting"));
          }
          return;
        }

        if (queueOfLists.size() == 1) {
          rollList(false);
        }
        
        Assert.assertTrue(queueOfLists.size() > 1, "Cannot peek from head of queue");
        BufferIterator itr = queueOfLists.peek().iterator();
        while (itr.hasNext()) {
          KeyToSeqNumObject entry = itr.next();
          if (logger.isTraceEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Peeking key " + entry + " from list " + System.identityHashCode(queueOfLists.peek())));
          }

          HDFSGatewayEventImpl ev = itr.value();
          ev.copyOffHeapValue();
          peekedEntries.add(ev);
        }
        
        // discard an empty batch as it is not processed and will plug up the
        // queue
        if (peekedEntries.isEmpty()) {
          SortedEventBuffer empty = queueOfLists.remove();
          if (logger.isTraceEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Discarding empty batch " + empty));
          }
          peeking.set(false);
        }
        this.lastPeekTimeInMillis = System.currentTimeMillis();
      }

      public HDFSGatewayEventImpl get(Region region, byte[] regionKey, long key) {
        KeyToSeqNumObject event = new KeyToSeqNumObject(regionKey, key);
        Iterator<SortedEventBuffer> queueIterator = queueOfLists.descendingIterator();
        while (queueIterator.hasNext()) {
          HDFSGatewayEventImpl evt = queueIterator.next().getFromQueueOrBuffer(event);
          if (evt != null) {
            return evt;
          }
        }
        return null;
      }
      
      public void handleRemainingElements(HashSet<Long> removedSeqNums) {
        if (!peeking.get()) {
          if (logger.isTraceEnabled() || VERBOSE) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Not peeked, just cleaning up empty batch; current list is " + currentSkipList));
          }
          return;
        }

        Assert.assertTrue(queueOfLists.size() > 1, "Cannot remove only event list");

        // all done with the peeked elements, okay to throw away now
        SortedEventBuffer buf = queueOfLists.remove();
        SortedEventBuffer.BufferIterator bufIter = buf.iterator();
        // Check if the removed buffer has any extra events. If yes, check if these extra 
        // events are part of region. If yes, reinsert these as they were probably inserted 
        // into this list while it was being peeked. 
        while (bufIter.hasNext()) {
          KeyToSeqNumObject key = bufIter.next();
          if (!removedSeqNums.contains(key.getSeqNum())) {
            HDFSGatewayEventImpl evt = (HDFSGatewayEventImpl) getNoLRU(key.getSeqNum(), true, false, false);
            if (evt != null) {
              flush.push();
              incQueueSize();
              queueOfLists.getFirst().add(key, evt.getSizeOnHDFSInBytes(!isBucketSorted));
            }
          }
        }

        decQueueSize(buf.size());
        flush.pop(buf.size());
        peeking.set(false);
      }
      
      public long getLastPeekTimeInMillis(){
        return this.lastPeekTimeInMillis;
      }
      
      NavigableSet<KeyToSeqNumObject> getPeeked() {
        assert peeking.get();
        return queueOfLists.peek().keySet();
      }
      
      private synchronized void rollList(boolean forceRollover) {
        if (currentSkipList.bufferSize() < batchSize && queueOfLists.size() > 1 && !forceRollover)
          return;
        SortedEventBuffer tmp = new SortedEventBuffer();
        queueOfLists.add(tmp);
        if (logger.isTraceEnabled() || VERBOSE) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "Rolling over list from " + currentSkipList + " to list " + tmp));
        }
        currentSkipList = tmp;
      }

      @Override
      public SortedEventQueueIterator iterator(Region region) {
        return new SortedEventQueueIterator(queueOfLists);
      }
    }
    
    public class SortedEventBuffer {
      private final HDFSGatewayEventImpl NULL = new HDFSGatewayEventImpl();
  
      private final ConcurrentSkipListMap<KeyToSeqNumObject, HDFSGatewayEventImpl> events = new ConcurrentSkipListMap<KeyToSeqNumObject, HDFSGatewayEventImpl>();
      
      private int bufferSize = 0;
      
      public boolean copyToBuffer(HDFSGatewayEventImpl event) {
        KeyToSeqNumObject key = new KeyToSeqNumObject(event.getSerializedKey(), event.getShadowKey());
        if (events.containsKey(key)) {
          // After an event has been delivered in a batch, we copy it into the
          // buffer so that it can be returned by an already in progress iterator.
          // If we do not do this it is possible to miss events since the hoplog
          // iterator uses a fixed set of files that are determined when the
          // iterator is created.  The events will be GC'd once the buffer is no
          // longer strongly referenced.
          HDFSGatewayEventImpl oldVal = events.put(key, event);
          assert oldVal == NULL;
  
          return true;
        }
        // If the primary lock is being relinquished, the events is cleared and probaly that is
        // why we are here. return true if the primary lock is being relinquished
        if (releasingPrimaryLock.get())
          return true;
        else 
          return false;
      }
  
      public HDFSGatewayEventImpl getFromQueueOrBuffer(KeyToSeqNumObject key) {
        KeyToSeqNumObject result = events.ceilingKey(key);
        if (result != null && Bytes.compareTo(key.getRegionkey(), result.getRegionkey()) == 0) {
          
          // first try to fetch the buffered event to make it fast. 
          HDFSGatewayEventImpl evt = events.get(result);
          if (evt != NULL) {
            return evt;
          }
          // now try to fetch the event from the queue region
          evt = (HDFSGatewayEventImpl) getNoLRU(result.getSeqNum(), true, false, false);
          if (evt != null) {
            return evt;
          }
          
          // try to fetch again from the buffered events to avoid a race between 
          // item deletion and the above two statements. 
          evt = events.get(result);
          if (evt != NULL) {
            return evt;
          }
          
        }
        return null;
      }
  
      public HDFSGatewayEventImpl add(KeyToSeqNumObject key, int sizeInBytes) {
        bufferSize += sizeInBytes;
        return events.put(key, NULL);
      }
  
      public void clear() {
        events.clear();
      }
  
      public boolean isEmpty() {
        return events.isEmpty();
      }
  
      public int bufferSize() {
        return bufferSize;
      }
      public int size() {
        return events.size();
      }
      public NavigableSet<KeyToSeqNumObject> keySet() {
        return events.keySet();
      }
  
      public BufferIterator iterator() {
        return new BufferIterator(events.keySet().iterator());
      }
  
      public class BufferIterator implements Iterator<KeyToSeqNumObject> {
        private final Iterator<KeyToSeqNumObject> src;

        private KeyToSeqNumObject currentKey;
        private HDFSGatewayEventImpl currentVal;

        private KeyToSeqNumObject nextKey;
        private HDFSGatewayEventImpl nextVal;
        
        public BufferIterator(Iterator<KeyToSeqNumObject> src) {
          this.src = src;
          moveNext();
        }
  
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean hasNext() {
          return nextVal != null;
        }
        
        @Override
        public KeyToSeqNumObject next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          
          currentKey = nextKey;
          currentVal = nextVal;
          
          moveNext();
          
          return currentKey;
        }
  
        public KeyToSeqNumObject key() {
          assert currentKey != null;
          return currentKey;
        }
        
        public HDFSGatewayEventImpl value() {
          assert currentVal != null;
          return currentVal;
        }
        
        private void moveNext() {
          while (src.hasNext()) {
            nextKey = src.next();
            nextVal = getFromQueueOrBuffer(nextKey);
            if (nextVal != null) {
              return;
            } else if (logger.isDebugEnabled() || VERBOSE) {
              logger.info(LocalizedMessage.create(LocalizedStrings.DEBUG, "The entry corresponding to"
                  + " the sequence number " + nextKey.getSeqNum() 
                  + " is missing. This can happen when an entry is already" 
                  + " dispatched before a bucket moved."));
            }
          }
          nextKey = null;
          nextVal = null;
        }
      }
    }
  
    public final class SortedEventQueueIterator implements CursorIterator<HDFSGatewayEventImpl> {
      /** the iterators to merge */
      private final List<SortedEventBuffer.BufferIterator> iters;
  
      /** the current iteration value */
      private HDFSGatewayEventImpl value;
  
      public SortedEventQueueIterator(Deque<SortedEventBuffer> queueOfLists) {
        iters = new ArrayList<SortedEventBuffer.BufferIterator>();
        for (Iterator<SortedEventBuffer> iter = queueOfLists.descendingIterator(); iter.hasNext();) {
          SortedEventBuffer.BufferIterator buf = iter.next().iterator();
          if (buf.hasNext()) {
            buf.next();
            iters.add(buf);
          }
        }
      }
      
      public void close() {
        value = null;
        iters.clear();
      }

      @Override
      public boolean hasNext() {
        return !iters.isEmpty();
      }
      
      @Override
      public HDFSGatewayEventImpl next() {
        if (!hasNext()) {
          throw new UnsupportedOperationException();
        }
        
        int diff = 0;
        KeyToSeqNumObject min = null;
        SortedEventBuffer.BufferIterator cursor = null;
        
        for (Iterator<SortedEventBuffer.BufferIterator> merge = iters.iterator(); merge.hasNext(); ) {
          SortedEventBuffer.BufferIterator buf = merge.next();
          KeyToSeqNumObject tmp = buf.key();
          if (min == null || (diff = Bytes.compareTo(tmp.regionkey, min.regionkey)) < 0) {
            min = tmp;
            cursor = buf;
            
          } else if (diff == 0 && !advance(buf, min)) {
            merge.remove();
          }
        }
        
        value = cursor.value();
        assert value != null;

        if (!advance(cursor, min)) {
          iters.remove(cursor);
        }
        return current();
      }
      
      @Override
      public final HDFSGatewayEventImpl current() {
        return value;
      }

      @Override 
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
      private boolean advance(SortedEventBuffer.BufferIterator iter, KeyToSeqNumObject key) {
        while (iter.hasNext()) {
          if (Bytes.compareTo(iter.next().regionkey, key.regionkey) > 0) {
            return true;
          }
        }
        return false;
      }
    }
}
