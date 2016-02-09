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
import java.util.List;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;


/**
 * Listener that persists events to HDFS
 *
 * @author Hemant Bhanawat
 */
public class HDFSEventListener implements AsyncEventListener {
  private final LogWriterI18n logger;
  private volatile boolean senderStopped = false;

  private final FailureTracker failureTracker = new FailureTracker(10L, 60 * 1000L, 1.5f);

  public HDFSEventListener(LogWriterI18n logger) {
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
    
    // The list of events that async queue receives are sorted at the
    // bucket level. Events for multiple regions are concatenated together.
    // Events for multiple buckets are sent which are concatenated
    // one after the other for e.g.
    //
    // <Region1, Key1, bucket1>, <Region1, Key19, bucket1>, 
    // <Region1, Key4, bucket2>, <Region1, Key6, bucket2>
    // <Region2, Key1, bucket1>, <Region2, Key4, bucket1>
    // ..
    
    Region previousRegion = null;
    int prevBucketId = -1;
    ArrayList<QueuedPersistentEvent> list = null;
    boolean success = false;
    try {
      //Back off if we are experiencing failures
      failureTracker.sleepIfRetry();
      
      HoplogOrganizer bucketOrganizer = null; 
      for (AsyncEvent asyncEvent : events) {
        if (senderStopped){
          failureTracker.failure();
          if (logger.fineEnabled()) {
            logger.fine("HDFSEventListener.processEvents: Cache is closing down. Ignoring the batch of data.");
          }
          return false;
        }
        HDFSGatewayEventImpl hdfsEvent = (HDFSGatewayEventImpl)asyncEvent;
        Region region = hdfsEvent.getRegion();
        
        if (prevBucketId != hdfsEvent.getBucketId() || region != previousRegion){
          if (prevBucketId != -1) {
            bucketOrganizer.flush(list.iterator(), list.size());
            success=true;
            if (logger.fineEnabled()) {
              logger.fine("Batch written to HDFS of size " + list.size() + " for region " + previousRegion);
            }
          }
          bucketOrganizer = getOrganizer((PartitionedRegion) region, hdfsEvent.getBucketId());
          // Bucket organizer can be null only when the bucket has moved. throw an exception so that the 
          // batch is discarded. 
          if (bucketOrganizer == null)
            throw new BucketMovedException("Bucket moved. BucketId: " + hdfsEvent.getBucketId() +  " HDFSRegion: " + region.getName());
          list = new  ArrayList<QueuedPersistentEvent>();
        }
        try {
          //TODO:HDFS check if there is any serialization overhead
          list.add(new SortedHDFSQueuePersistedEvent(hdfsEvent));
        } catch (ClassNotFoundException e) {
          //TODO:HDFS add localized string
          logger.warning(new StringId(0, "Error while converting HDFSGatewayEvent to PersistedEventImpl."), e);
          return false;
        }
        prevBucketId = hdfsEvent.getBucketId();
        previousRegion = region;
        
      }
      if (bucketOrganizer != null) {
        bucketOrganizer.flush(list.iterator(), list.size());
        success = true;
        
        if (logger.fineEnabled()) {
          logger.fine("Batch written to HDFS of size " + list.size() + " for region " + previousRegion);
        }
      }
    } catch (IOException e) {
      logger.warning(LocalizedStrings.HOPLOG_FLUSH_FOR_BATCH_FAILED, e);
      return false;
    }
    catch (ForceReattemptException e) {
      if (logger.fineEnabled())
        logger.fine(e);
      return false;
    }
    catch(PrimaryBucketException e) {
      //do nothing, the bucket is no longer primary so we shouldn't get the same
      //batch next time.
      if (logger.fineEnabled())
        logger.fine(e);
      return false;
    }
    catch(BucketMovedException e) {
      //do nothing, the bucket is no longer primary so we shouldn't get the same
      //batch next time.
      if (logger.fineEnabled())
        logger.fine(e);
      return false;
    }
    catch (CacheClosedException e) {
      if (logger.fineEnabled())
        logger.fine(e);
      // exit silently
      return false;
    } catch (InterruptedException e1) {
      if (logger.fineEnabled())
        logger.fine(e1);
      return false;
    } finally {
      failureTracker.record(success);
    }

    return true;
  }
  
  private HoplogOrganizer getOrganizer(PartitionedRegion region, int bucketId) {
    BucketRegion br = region.getDataStore().getLocalBucketById(bucketId);
    if (br == null) {
      // got rebalanced or something
      throw new PrimaryBucketException("Bucket region is no longer available " + bucketId + region);
    }

    return br.getHoplogOrganizer();
  }

}
