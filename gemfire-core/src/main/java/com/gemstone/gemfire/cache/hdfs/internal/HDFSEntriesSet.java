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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSBucketRegionQueue.SortedEventQueueIterator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.HDFSRegionMap;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion.IteratorType;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("rawtypes")
public class HDFSEntriesSet extends AbstractSet {
  private final IteratorType type;

  private final HoplogOrganizer hoplogs;
  private final HDFSBucketRegionQueue brq;
  
  private final BucketRegion region; 
  private final ReferenceQueue<HDFSIterator> refs;
  
  public HDFSEntriesSet(BucketRegion region, HDFSBucketRegionQueue brq, 
      HoplogOrganizer hoplogs, IteratorType type, ReferenceQueue<HDFSIterator> refs) {
    this.region = region;
    this.brq = brq;
    this.hoplogs = hoplogs;
    this.type = type;
    this.refs = refs;
  }
  
  @Override
  public HDFSIterator iterator() {
    HDFSIterator iter = new HDFSIterator(type, region.getPartitionedRegion(), true);
    if (refs != null) {
      // we can't rely on an explicit close but we need to free resources
      //
      // This approach has the potential to cause excessive memory load and/or
      // GC problems if an app holds an iterator ref too long. A lease-based
      // approach where iterators are automatically for X secs of inactivity is
      // a potential alternative (but may require tuning for certain
      // applications)
      new WeakReference<HDFSEntriesSet.HDFSIterator>(iter, refs);
    }
    return iter;
  }

  @Override
  public int size() {
    // TODO this is the tortoise version, need a fast version for estimation
    // note: more than 2^31-1 records will cause this counter to wrap
    int size = 0;
    HDFSIterator iter = new HDFSIterator(null, region.getPartitionedRegion(), false);
    try {
      while (iter.hasNext()) {
        if (includeEntry(iter.next())) {
          size++;
        }
      }
    } finally {
      iter.close();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    HDFSIterator iter = new HDFSIterator(null, region.getPartitionedRegion(), false);
    try {
      while (iter.hasNext()) {
        if (includeEntry(iter.next())) {
          return false;
        }
      }
    } finally {
      iter.close();
    }
    return true;
  }

  private boolean includeEntry(Object val) {
    if (val instanceof HDFSGatewayEventImpl) {
      HDFSGatewayEventImpl evt = (HDFSGatewayEventImpl) val;
      if (evt.getOperation().isDestroy()) {
        return false;
      }
    } else if (val instanceof PersistedEventImpl) {
      PersistedEventImpl evt = (PersistedEventImpl) val;
      if (evt.getOperation().isDestroy()) {
        return false;
      }
    }
    return true;
  }

  public class HDFSIterator implements Iterator {
    private final IteratorType type;
    private final boolean deserialize;
    
    private final SortedEventQueueIterator queue;
    private final HoplogIterator<byte[], SortedHoplogPersistedEvent> hdfs;
    private Iterator txCreatedEntryIterator;
    
    private boolean queueNext;
    private boolean hdfsNext;
    private boolean forUpdate;
    private boolean hasTxEntry;

    private byte[] currentHdfsKey;

    public HDFSIterator(IteratorType type, Region region, boolean deserialize) {
      this.type = type;
      this.deserialize = deserialize;

      // Check whether the queue has become primary here.
      // There could be some time between bucket becoming a primary 
      // and underlying queue becoming a primary, so isPrimaryWithWait() 
      // waits for some time for the queue to become a primary on this member
      if (!brq.getBucketAdvisor().isPrimaryWithWait()) {
        InternalDistributedMember primaryHolder = brq.getBucketAdvisor()
            .basicGetPrimaryMember();
        throw new PrimaryBucketException("Bucket " + brq.getName()
            + " is not primary. Current primary holder is " + primaryHolder);
      }
      // We are deliberating NOT sync'ing while creating the iterators.  If done
      // in the correct order, we may get duplicates (due to an in-progress
      // flush) but we won't miss any entries.  The dupes will be eliminated
      // during iteration.
      queue = brq.iterator(region);
      advanceQueue();
      
      HoplogIterator<byte[], SortedHoplogPersistedEvent> tmp = null;
      try {
        tmp = hoplogs.scan();
      } catch (IOException e) {
        HDFSEntriesSet.this.region.checkForPrimary();
        throw new HDFSIOException(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(e.getMessage()), e);
      }
      
      hdfs = tmp;
      if (hdfs != null) {
        advanceHdfs();
      }
    }
    
    @Override
    public boolean hasNext() {
      boolean nonTxHasNext = hdfsNext || queueNext;
      if (!nonTxHasNext && this.txCreatedEntryIterator != null) {
        this.hasTxEntry = this.txCreatedEntryIterator.hasNext();
        return this.hasTxEntry;
      }
      return nonTxHasNext;
    }
    
    @Override
    public Object next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (hasTxEntry) {
        hasTxEntry = false;
        return this.txCreatedEntryIterator.next();
      }

      Object val;
      if (!queueNext) {
        val = getFromHdfs();
        advanceHdfs();
        
      } else if (!hdfsNext) {
        val = getFromQueue();
        advanceQueue();
        
      } else {
        byte[] qKey = queue.current().getSerializedKey();
        byte[] hKey = this.currentHdfsKey;
        
        int diff = Bytes.compareTo(qKey, hKey);
        if (diff < 0) {
          val = getFromQueue();
          advanceQueue();
          
        } else if (diff == 0) {
          val = getFromQueue();
          advanceQueue();

          // ignore the duplicate
          advanceHdfs();

        } else {
          val = getFromHdfs();
          advanceHdfs();
        }
      }
      return val;
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
    public void close() {
      if (queueNext) {
        queue.close();
      }

      if (hdfsNext) {
        hdfs.close();
      }
    }

    private Object getFromQueue() {
      HDFSGatewayEventImpl evt = queue.current();
      if (type == null) {
        return evt;
      }
      
      switch (type) {
      case KEYS:
        byte[] key = evt.getSerializedKey();
        return deserialize ? EntryEventImpl.deserialize(key) : key;
        
      case VALUES:
        return evt.getValue();
        
      default:
        Object keyObj = EntryEventImpl.deserialize(evt.getSerializedKey());
        if(keyObj instanceof KeyWithRegionContext) {
          ((KeyWithRegionContext)keyObj).setRegionContext(region.getPartitionedRegion());
        }
        return ((HDFSRegionMap) region.getRegionMap()).getDelegate().getEntryFromEvent(keyObj, evt, true, forUpdate);
      }
    }

    private Object getFromHdfs() {
      if (type == null) {
        return hdfs.getValue();
      }
      
      switch (type) {
      case KEYS:
        byte[] key = this.currentHdfsKey;
        return deserialize ? EntryEventImpl.deserialize(key) : key;
        
      case VALUES:
        PersistedEventImpl evt = hdfs.getValue();
        return evt.getValue();
        
      default:
        Object keyObj = EntryEventImpl.deserialize(this.currentHdfsKey);
        if(keyObj instanceof KeyWithRegionContext) {
          ((KeyWithRegionContext)keyObj).setRegionContext(region.getPartitionedRegion());
        }
        return ((HDFSRegionMap) region.getRegionMap()).getDelegate().getEntryFromEvent(keyObj, hdfs.getValue(), true, forUpdate);
      }
    }
    
    private void advanceHdfs() {
      if (hdfsNext = hdfs.hasNext()) {
        try {
          this.currentHdfsKey = hdfs.next();
        } catch (IOException e) {
          region.checkForPrimary();
          throw new HDFSIOException(LocalizedStrings.HOPLOG_FAILED_TO_READ_HDFS_FILE.toLocalizedString(e.getMessage()), e);
        }
      } else {
        this.currentHdfsKey = null;
        hdfs.close();
      }
    }
    
    private void advanceQueue() {
      if (queueNext = queue.hasNext()) {
        queue.next();
      } else {
        brq.checkForPrimary();
        queue.close();
      }
    }
    
    public void setForUpdate(){
      this.forUpdate = true;
    }
    
    /**MergeGemXDHDFSToGFE not sure of this function is required */ 
    /*public void setTXState(TXState txState) {
      TXRegionState txr = txState.getTXRegionState(region);
      if (txr != null) {
        txr.lock();
        try {
          this.txCreatedEntryIterator = txr.getCreatedEntryKeys().iterator();
        }
        finally{
          txr.unlock();
        }
      }
    }*/
  }
}
