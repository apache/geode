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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Listener that persists events to a write only HDFS store
 *
 * @author Hemant Bhanawat
 */
public class HDFSWriteOnlyStoreEventListener implements
    AsyncEventListener {

  private final LogWriterI18n logger;
  private volatile boolean senderStopped = false; 
  private final FailureTracker failureTracker = new FailureTracker(10L, 60 * 1000L, 1.5f);
  
  
  public HDFSWriteOnlyStoreEventListener(LogWriterI18n logger) {
    this.logger = logger;
  }
  
  @Override
  public void close() {
    senderStopped = true;
  }

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    if (Hoplog.NOP_WRITE) {
      return true;
    }

    if (logger.fineEnabled())
      logger.fine("HDFSWriteOnlyStoreEventListener: A total of " + events.size() + " events are sent from GemFire to persist on HDFS");
    boolean success = false;
    try {
      failureTracker.sleepIfRetry();
      HDFSGatewayEventImpl hdfsEvent = null;
      int previousBucketId = -1;
      BatchManager bm = null;
      for (AsyncEvent asyncEvent : events) {
        if (senderStopped){
          if (logger.fineEnabled()) {
            logger.fine("HDFSWriteOnlyStoreEventListener.processEvents: Cache is closing down. Ignoring the batch of data.");
          }
          return false;
        }
        hdfsEvent = (HDFSGatewayEventImpl)asyncEvent;
        if (previousBucketId != hdfsEvent.getBucketId()){
          if (previousBucketId != -1) 
            persistBatch(bm, previousBucketId);
          
          previousBucketId = hdfsEvent.getBucketId();
          bm = new BatchManager();
        }
        bm.addEvent(hdfsEvent);
      }
      try {
        persistBatch(bm, hdfsEvent.getBucketId());
      } catch (BucketMovedException e) {
        logger.fine("Batch could not be written to HDFS as the bucket moved. bucket id: " + 
            hdfsEvent.getBucketId() + " Exception: " + e);
        return false;
      }
      success = true;
    } catch (IOException e) {
      logger.warning(LocalizedStrings.HOPLOG_FLUSH_FOR_BATCH_FAILED, e);
      return false;
    }
    catch (ClassNotFoundException e) {
      logger.warning(LocalizedStrings.HOPLOG_FLUSH_FOR_BATCH_FAILED, e);
      return false;
    }
    catch (CacheClosedException e) {
      // exit silently
      if (logger.fineEnabled())
        logger.fine(e);
      return false;
    } catch (ForceReattemptException e) {
      if (logger.fineEnabled())
        logger.fine(e);
      return false;
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } finally {
      failureTracker.record(success);
    }
    return true;
  }
  
  /**
   * Persists batches of multiple regions specified by the batch manager
   * 
   */
  private void persistBatch(BatchManager bm, int bucketId) throws IOException, ForceReattemptException {
    Iterator<Map.Entry<LocalRegion,ArrayList<QueuedPersistentEvent>>> eventsPerRegion = 
        bm.iterator();
    HoplogOrganizer bucketOrganizer = null; 
    while (eventsPerRegion.hasNext()) {
      Map.Entry<LocalRegion, ArrayList<QueuedPersistentEvent>> eventsForARegion = eventsPerRegion.next();
      bucketOrganizer = getOrganizer((PartitionedRegion) eventsForARegion.getKey(), bucketId);
      // bucket organizer cannot be null. 
      if (bucketOrganizer == null)
        throw new BucketMovedException("Bucket moved. BucketID: " + bucketId + "  HdfsRegion: " +  eventsForARegion.getKey().getName());
      bucketOrganizer.flush(eventsForARegion.getValue().iterator(), eventsForARegion.getValue().size());
      if (logger.fineEnabled()) {
        logger.fine("Batch written to HDFS of size " +  eventsForARegion.getValue().size() + 
            " for region " + eventsForARegion.getKey());
      }
    }
  }

  private HoplogOrganizer getOrganizer(PartitionedRegion region, int bucketId) {
    BucketRegion br = region.getDataStore().getLocalBucketById(bucketId);
    if (br == null) {
      // got rebalanced or something
      throw new BucketMovedException("Bucket region is no longer available. BucketId: "+
          bucketId + " HdfsRegion: " +  region.getName());
    }

    return br.getHoplogOrganizer();
  }
  
  /**
   * Sorts out events of the multiple regions into lists per region 
   *
   */
  private class BatchManager implements Iterable<Map.Entry<LocalRegion,ArrayList<QueuedPersistentEvent>>> {
    private HashMap<LocalRegion, ArrayList<QueuedPersistentEvent>> regionBatches = 
        new HashMap<LocalRegion, ArrayList<QueuedPersistentEvent>>();
    
    public void addEvent (HDFSGatewayEventImpl hdfsEvent) throws IOException, ClassNotFoundException {
      LocalRegion region = (LocalRegion) hdfsEvent.getRegion();
      ArrayList<QueuedPersistentEvent> regionList = regionBatches.get(region);
      if (regionList == null) {
        regionList = new ArrayList<QueuedPersistentEvent>();
        regionBatches.put(region, regionList);
      }
      regionList.add(new UnsortedHDFSQueuePersistedEvent(hdfsEvent));
    }

    @Override
    public Iterator<Map.Entry<LocalRegion,ArrayList<QueuedPersistentEvent>>> iterator() {
      return regionBatches.entrySet().iterator();
    }
    
  }
}
