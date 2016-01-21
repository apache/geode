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
package com.gemstone.gemfire.internal.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestRegistrationEvent;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.query.internal.IndexUpdater;
import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.cache.query.internal.index.IndexCreationData;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketRegion.RawValue;
import com.gemstone.gemfire.internal.cache.LocalRegion.RegionPerfStats;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.BucketLock;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.SizeEntry;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.execute.PartitionedRegionFunctionResultSender;
import com.gemstone.gemfire.internal.cache.execute.RegionFunctionContextImpl;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionObserver;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionObserverHolder;
import com.gemstone.gemfire.internal.cache.partitioned.RedundancyAlreadyMetException;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketResponse;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock.StoppableReadLock;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock.StoppableWriteLock;
import com.gemstone.gemfire.i18n.StringId;

/**
 * Implementation of DataStore (DS) for a PartitionedRegion (PR). This will be
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
 * accessed via accessor of PartitionedRegion or PartionService thread which
 * will handle remote calls to this DataStore from other nodes participating in
 * this PartitionedRegion.
 * 
 * @author rreja
 * @author tapshank
 * @author Mitch Thomas
 */
public class PartitionedRegionDataStore implements HasCachePerfStats
{
  private static final Logger logger = LogService.getLogger();
  
  /** PR reference for this DataStore */
  protected final PartitionedRegion partitionedRegion;

  /**
   * Total memory used by this partition. Only used for statistics.
   */
  final private AtomicLong bytesInUse = new AtomicLong(0);

  /**
   * CacheLoader of this PRDataStore
   */
  private CacheLoader loader;

  /**
   * Anyone creating a bucket must hold the read lock.  Anyone deleting
   * a bucket must hold the write lock.
   */
  final StoppableReentrantReadWriteLock bucketCreationLock;
  
  /**
   * Maps each bucket to a real region that contains actual key/value entries
   * for this PR instance
   * <p>
   * Keys are instances of {@link Integer}.
   * Values are instances of (@link BucketRegion}.
   */
  final ConcurrentMap<Integer, BucketRegion> localBucket2RegionMap;
  
  /**
   * A counter of the number of concurrent bucket creates in progress on
   * this node
   */
  final AtomicInteger bucketCreatesInProgress = new AtomicInteger();
  
  /**
   * Variable used to determine when the data store is over the local max memory limit
   */
  private boolean exceededLocalMaxMemoryLimit = false;
  
  /**
   * Maximum number of bytes this dataStore has for storage
   */
  private final long maximumLocalBytes; 
  
  final private CachePerfStats bucketStats;

  /**
   * The keysOfInterest contains a set of all keys in which any client has
   * interest in this PR.
   */
  final ConcurrentMap keysOfInterest;

  private final Object keysOfInterestLock = new Object();

  /**
   * Update an entry's last access time if a client is interested in the entry.
   */
  private static final boolean UPDATE_ACCESS_TIME_ON_INTEREST = Boolean
      .getBoolean("gemfire.updateAccessTimeOnClientInterest");

  /**
   * Creates PartitionedRegionDataStore for dataStorage of PR and starts a
   * PartitionService to handle remote operations on this DataStore from other
   * participating nodes.
   * 
   * @param pr
   *          PartitionedRegion associated with this DataStore.
   */
  PartitionedRegionDataStore(final PartitionedRegion pr) {
    final int bucketCount = pr.getTotalNumberOfBuckets();
    this.localBucket2RegionMap = new ConcurrentHashMap<Integer, BucketRegion>(bucketCount);
    this.partitionedRegion = pr;
    this.bucketCreationLock = new StoppableReentrantReadWriteLock(pr.getCancelCriterion());
    if (pr.getAttributes().getCacheLoader() != null) {
      this.loader = pr.getAttributes().getCacheLoader();
      if (logger.isDebugEnabled()) {
        logger.debug("Installing cache loader from partitioned region attributes: {}", loader);
      }
    }
//  this.maximumLocalBytes =  (long) (pr.getLocalMaxMemory() * PartitionedRegionHelper.BYTES_PER_MB
//  * this.partitionedRegion.rebalanceThreshold);
    this.maximumLocalBytes =  (pr.getLocalMaxMemory() * PartitionedRegionHelper.BYTES_PER_MB);
    
//    this.bucketStats = new CachePerfStats(pr.getSystem(), "partition-" + pr.getName());
    this.bucketStats = new RegionPerfStats(pr.getCache(), pr.getCachePerfStats(), "partition-" + pr.getName());
    this.keysOfInterest = new ConcurrentHashMap();
  }

  /**
   * This method creates a PartitionedRegionDataStore be invoking the PRDS
   * Constructor.
   * 
   * @param cache
   * @param pr
   * @param pa
   * @return @throws
   *         PartitionedRegionException
   */
  static PartitionedRegionDataStore createDataStore(Cache cache,
      PartitionedRegion pr, PartitionAttributes pa)
      throws PartitionedRegionException
  {
    PartitionedRegionDataStore prd = new PartitionedRegionDataStore(pr);    
    return prd;
  }

//  /**
//   * Checks whether there is room in this Map to accommodate more data without
//   * pushing the Map over its rebalance threshold.
//   * 
//   * @param bytes
//   *          the size to check in bytes
//   */
//  boolean canAccommodateMoreBytesSafely(long bytes)
//  {
//
//    if (this.partitionedRegion.getLocalMaxMemory() == 0) {
//      return false;
//    }
//    long allocatedMemory = currentAllocatedMemory();
//    // precision coercion from int to long on bytes
//    long newAllocatedSize = allocatedMemory + bytes;
//    if (newAllocatedSize < (this.partitionedRegion.getLocalMaxMemory()
//        * PartitionedRegionHelper.BYTES_PER_MB * this.partitionedRegion.rebalanceThreshold)) {
//      return true;
//    }
//    else {
//      return false;
//    }
//  }

  /**
   * Test to determine if this data store is managing a bucket
   * 
   * @param bucketId
   *          the id of the bucket
   * @return true if the provided bucket is being managed
   */
  public final boolean isManagingBucket(int bucketId)
  {
    BucketRegion buk = this.localBucket2RegionMap.get(Integer.valueOf(bucketId));    
    if (buk != null && !buk.isDestroyed()) {
      return true;
    }
    return false;
  }
  
  /**
   * Report the number of buckets currently managed by this DataStore
   */
  public short getBucketsManaged() {
    return (short) this.localBucket2RegionMap.size();
  }

  /**
   * Report an estimate of the number of primary buckets managed locally
   * The result of this method is not stable.
   */
  public int getNumberOfPrimaryBucketsManaged() {
    final AtomicInteger numPrimaries = new AtomicInteger();
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region r) {
        BucketRegion br = (BucketRegion) r;
        if (br.getBucketAdvisor().isPrimary()) {
          numPrimaries.incrementAndGet();
        }
      }
    });
    return numPrimaries.get();
  }
  
  
  /**
   * Indicates if this data store is managing buckets
   * 
   * @return true if it is managing buckets
   */
  final boolean isManagingAnyBucket()
  {
    return !this.localBucket2RegionMap.isEmpty();
  }
  
  /** Try to grab buckets for all the colocated regions
  /* In case we can't grab buckets there is no going back
   * @param creationRequestor 
   * @param isDiskRecovery
   */
  
  protected CreateBucketResult grabFreeBucketRecursively(final int bucketId, 
                                              final PartitionedRegion pr,     
                                              final InternalDistributedMember moveSource, 
                                              final boolean forceCreation, 
                                              final boolean isRebalance,
                                              final boolean replaceOfflineData, 
                                              final InternalDistributedMember creationRequestor, 
                                              final boolean isDiskRecovery) {
    CreateBucketResult  grab;
    DistributedMember dm = pr.getMyId();
    List<PartitionedRegion> colocatedWithList = ColocationHelper.getColocatedChildRegions(pr);
    //make sure we force creation and ignore redundancy checks for the child region.
    //if we created the parent bucket, we want to make sure we create the child bucket.
    grab = pr.getDataStore().grabFreeBucket(bucketId, dm, 
        null, true, isRebalance, true, replaceOfflineData, creationRequestor);
    if (!grab.nowExists()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed grab for bucketId = {}{}{}", pr.getPRId(), pr.BUCKET_ID_SEPARATOR, bucketId);
      }
      // Assert.assertTrue(nList.contains(partitionedRegion.getNode().getMemberId())
      // ,
      // " grab returned false and b2n does not contains this member.");
    }
    if(colocatedWithList != null) {
      Iterator<PartitionedRegion> itr = colocatedWithList.iterator();
      while (itr.hasNext()) {
        PartitionedRegion coLocatedWithPr = itr.next();
        if((isDiskRecovery || coLocatedWithPr.isInitialized()) 
            && coLocatedWithPr.getDataStore().isColocationComplete(bucketId)) {
          grab = coLocatedWithPr.getDataStore().grabFreeBucketRecursively(
              bucketId, coLocatedWithPr, moveSource, forceCreation, 
              isRebalance, replaceOfflineData, creationRequestor, isDiskRecovery);

          if (!grab.nowExists()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Failed grab for bucketId = {}{}{}", pr.getPRId(), pr.BUCKET_ID_SEPARATOR, bucketId);
            }
            // Assert.assertTrue(nList.contains(partitionedRegion.getNode().getMemberId())
            // ,
            // " grab returned false and b2n does not contains this member.");
          }
        }
      }
    }
    return grab;
  }
  
  /**
   * Attempts to map a bucket id to this node. Creates real storage for the
   * bucket by adding a new Region to bucket2Map. Bucket creation is done under
   * the d-lock on b2n region.
   * 
   * @param possiblyFreeBucketId  the identity of the bucket
   + @param mustBeNew  boolean enforcing that the bucket must not already exist 
   * @param sender  the member requesting the bucket 
   * @param moveSource Where we are moving the bucket from, if this is a move.
   * @param forceCreation avoid any checks (with in reason) which might prevent bucket creation
   * @param isRebalance true if bucket creation is directed by rebalancing    
   * @param replaceOffineData 
   * @return true if successful
   */
  CreateBucketResult grabFreeBucket(final int possiblyFreeBucketId, 
                         final DistributedMember sender,
                         final InternalDistributedMember moveSource, 
                         final boolean forceCreation, 
                         final boolean isRebalance,
                         final boolean lockRedundancyLock, 
                         boolean replaceOffineData,
                         InternalDistributedMember creationRequestor) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    long startTime
        = this.partitionedRegion.getPrStats().startBucketCreate(isRebalance);
    boolean createdBucket = false;
    PartitionedRegionObserver observer = PartitionedRegionObserverHolder.getInstance();
    observer.beforeBucketCreation(this.partitionedRegion, possiblyFreeBucketId);
    try {
    
      CreateBucketResult result = CreateBucketResult.FAILED;
      if (isManagingBucket(possiblyFreeBucketId)) {
        if (isDebugEnabled) {
          logger.debug("grabFreeBucket: VM {} already contains the bucket with bucketId={}{}{}", this.partitionedRegion.getMyId(),
              partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId);
        }
        this.partitionedRegion.checkReadiness();
        return CreateBucketResult.ALREADY_EXISTS;
      }
      StoppableReadLock parentBucketCreationLock = getParentBucketCreationLock();
      if (parentBucketCreationLock != null) {
        parentBucketCreationLock.lock();
      }
      
      try {
        if (!okToCreateChildBucket(possiblyFreeBucketId)) {
          return CreateBucketResult.FAILED;
        }
        StoppableReadLock lock = this.bucketCreationLock.readLock();
        lock.lock();  // prevent destruction while creating buckets
        try {
          //This counter is used by canAccomodateAnotherBucket to estimate if this member should
          //accept another bucket
          bucketCreatesInProgress.incrementAndGet();
          if (this.partitionedRegion.isDestroyed()) {
            return CreateBucketResult.FAILED;
          }
          
  //        final boolean needsAllocation;
          if (isDebugEnabled) {
            this.logger.debug("grabFreeBucket: node list {} for bucketId={}{}{}",
                PartitionedRegionHelper.printCollection(this.partitionedRegion.getRegionAdvisor().getBucketOwners(possiblyFreeBucketId)),
                partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId);
          }


          // Final accommodation check under synchronization for
          // a stable view of bucket creation
          if (! forceCreation ) { 
            // Currently the balancing check should only run when creating buckets, 
            // as opposed to during bucket recovery... it is assumed that 
            // ifRedudnancyNotSatisfied is false during bucket recovery
            if (! canAccommodateAnotherBucket()) {
              result = CreateBucketResult.FAILED;
              return result;
            }
          }

          ProxyBucketRegion buk = partitionedRegion.getRegionAdvisor().getProxyBucketArray()[possiblyFreeBucketId];
          //Prevent other threads on the same VM from creating this bucket.
          //It doesn't look the the redundancy lock actually correctly
          //handles multiple threads, and the isManagingBucket call
          //also needs to be done under this lock
          synchronized(buk) {
            //DAN - this just needs to be done holding a lock for this particular bucket
            if(!verifyBucketBeforeGrabbing(possiblyFreeBucketId)) {
              result = CreateBucketResult.FAILED;
              return result;
            }
            
            if (!this.isManagingBucket(possiblyFreeBucketId)) {
              Integer possiblyFreeBucketIdInt = Integer
              .valueOf(possiblyFreeBucketId);
              
              BucketRegion bukReg = null;
              Object redundancyLock = lockRedundancyLock(moveSource,
                  possiblyFreeBucketId, replaceOffineData);
              //DAN - I hope this is ok to do without that bucket admin lock
              // Take SQLF lock to wait for any ongoing index initializations.
              // The lock is taken here in addition to that in
              // DistributedRegion#initialize() so as to release only after
              // assignBucketRegion() has been invoked (see bug #41877).
              // Assumes that the IndexUpdater#lockForGII() lock is re-entrant.
              final IndexUpdater indexUpdater = this.partitionedRegion
              .getIndexUpdater();
              boolean sqlfIndexLocked = false;
              try {
                if (indexUpdater != null) {
                  indexUpdater.lockForGII();
                  sqlfIndexLocked = true;
                }
                buk.initializePrimaryElector(creationRequestor);
                if (getPartitionedRegion().getColocatedWith() == null) {
                  buk.getBucketAdvisor().setShadowBucketDestroyed(false);
                }
                if (getPartitionedRegion().isShadowPR()) {
                  getPartitionedRegion().getColocatedWithRegion()
                  .getRegionAdvisor()
                  .getBucketAdvisor(possiblyFreeBucketId)
                  .setShadowBucketDestroyed(false);
                }
                bukReg = createBucketRegion(possiblyFreeBucketId);
                //Mark the bucket as hosting and distribute to peers
                //before we release the dlock. This makes sure that our peers
                //won't think they need to satisfy redundancy
                if (bukReg != null) {
                  // Let the data store know about the real bucket at this point
                  // so that other VMs which discover the real bucket via a
                  // profile exchange can send messages to the data store and
                  // safely use the bucket.
                  observer.beforeAssignBucket(this.partitionedRegion, possiblyFreeBucketId);
                  assignBucketRegion(bukReg.getId(), bukReg);
                  buk.setHosting(true);
                  bukReg.invokePartitionListenerAfterBucketCreated();
                }
                else {
                  if (buk.getPartitionedRegion().getColocatedWith() == null) {
                    buk.getBucketAdvisor().setShadowBucketDestroyed(true);
                    // clear tempQueue for all the shadowPR buckets
                    clearAllTempQueueForShadowPR(buk.getBucketId());
                  }
                }
              } finally {
                if (sqlfIndexLocked) {
                  indexUpdater.unlockForGII();
                }
                releaseRedundancyLock(redundancyLock);
                if(bukReg == null) {
                  buk.clearPrimaryElector();
                }
              }

              if (bukReg != null) {
                if (isDebugEnabled) {
                  logger.debug("grabFreeBucket: mapped bucketId={}{}{} on node = {}", this.partitionedRegion.getPRId(),
                      PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId, this.partitionedRegion.getMyId());
                }

                createdBucket = true;
                result = CreateBucketResult.CREATED;
              } 
              else {
                Assert.assertTrue(this.localBucket2RegionMap.get(possiblyFreeBucketIdInt) == null);
                result = CreateBucketResult.FAILED;
              }
            }
            else {
              // Handle the case where another concurrent thread decided to manage
              // the bucket and the creator may have died 
              if (isDebugEnabled) {
                logger.debug("grabFreeBucket: bucketId={}{}{} already mapped on VM = {}", this.partitionedRegion.getPRId(),
                    PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId, partitionedRegion.getMyId());
              }
              result = CreateBucketResult.ALREADY_EXISTS;
            }
            if (isDebugEnabled) {
              logger.debug("grabFreeBucket: Mapped bucketId={}{}{}", this.partitionedRegion.getPRId(),
                  PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId);
            }
          }
        }
        catch (RegionDestroyedException rde) {
          RegionDestroyedException rde2 = new RegionDestroyedException(toString(),
              this.partitionedRegion.getFullPath());
          rde2.initCause(rde);
          throw rde2;
        }
        catch(RedundancyAlreadyMetException e) {
          if (isDebugEnabled) {
            logger.debug("Redundancy already met {}{}{} assignment {}", this.partitionedRegion.getPRId(),
                PartitionedRegion.BUCKET_ID_SEPARATOR, possiblyFreeBucketId, localBucket2RegionMap.get(Integer.valueOf(possiblyFreeBucketId)));
          }
          result = CreateBucketResult.REDUNDANCY_ALREADY_SATISFIED;
        }
        finally {
          bucketCreatesInProgress.decrementAndGet();
          lock.unlock();  // prevent destruction while creating buckets
        }
      } finally {
        if (parentBucketCreationLock != null) {
          parentBucketCreationLock.unlock();
        }
      }
      this.partitionedRegion.checkReadiness();
      this.partitionedRegion.checkClosed();
      
      return result;
    
    } finally {
      this.partitionedRegion.getPrStats().endBucketCreate(
          startTime, createdBucket, isRebalance);
    }
  }

  protected void clearAllTempQueueForShadowPR(final int bucketId) {
    List<PartitionedRegion> colocatedWithList = ColocationHelper
        .getColocatedChildRegions(partitionedRegion);
    for (PartitionedRegion childRegion : colocatedWithList) {
      if (childRegion.isShadowPR()) {
        AbstractGatewaySender sender = childRegion
            .getParallelGatewaySender();
        if (sender == null) {
          return;
        }
        AbstractGatewaySenderEventProcessor eventProcessor = sender
            .getEventProcessor();
        if (eventProcessor == null) {
          return;
        }

        ConcurrentParallelGatewaySenderQueue queue = (ConcurrentParallelGatewaySenderQueue)eventProcessor
            .getQueue();

        if (queue == null) {
          return;
        }
        BlockingQueue<GatewaySenderEventImpl> tempQueue = queue
        		.getBucketTmpQueue(bucketId);
        if (tempQueue != null) {
          synchronized (tempQueue) {
            for (GatewaySenderEventImpl event : tempQueue) {
              event.release();
            }
            tempQueue.clear();  
          }
        }
      }
    }
  }
  
  public Object lockRedundancyLock(InternalDistributedMember moveSource, int bucketId, boolean replaceOffineData) {
    //TODO prperist - Make this thing easier to find!
    final PartitionedRegion.BucketLock bl = partitionedRegion
        .getRegionAdvisor().getBucketAdvisor(bucketId).getProxyBucketRegion()
        .getBucketLock();
    bl.lock();
    boolean succeeded =false;
    try {
      ProxyBucketRegion buk = partitionedRegion.getRegionAdvisor().getProxyBucketArray()[bucketId];
      if (!buk.checkBucketRedundancyBeforeGrab(moveSource, replaceOffineData)) {
        if(logger.isDebugEnabled()) {
          logger.debug("Redundancy already satisfied. current owners=", partitionedRegion.getRegionAdvisor().getBucketOwners(bucketId));
        }
        throw new RedundancyAlreadyMetException();
      }
      succeeded=true;
    } finally {
      if(!succeeded) {
        bl.unlock();
      }
    }
    
    return bl;
  }
  
  public void releaseRedundancyLock(Object lock) {
    try {

    } finally {
      PartitionedRegion.BucketLock bl  = (BucketLock) lock;
      bl.unlock();
    }
  }

  private StoppableReadLock getParentBucketCreationLock() {
    PartitionedRegion colocatedRegion = 
      ColocationHelper.getColocatedRegion(this.partitionedRegion);
    StoppableReadLock parentLock = null; 
    if (colocatedRegion != null) {
      parentLock = colocatedRegion.getDataStore().bucketCreationLock.readLock();
      return parentLock;
    }
    return null;
  }

  /**
   * Returns false if this region is colocated and parent bucket does not 
   * exist.
   * @return true if ok to make bucket
   */
  private boolean okToCreateChildBucket(int bucketId) {
    PartitionedRegion colocatedRegion = 
      ColocationHelper.getColocatedRegion(this.partitionedRegion);
    if (colocatedRegion != null &&
        !colocatedRegion.getDataStore().isManagingBucket(bucketId)) {
      if (logger.isDebugEnabled()) {
        logger.debug("okToCreateChildBucket - we don't manage the parent bucket");
      }
      return false;
    }
    if(!isColocationComplete(bucketId)) {
      return false;
    }
    
    return true;
  }
  
  private boolean isColocationComplete(int bucketId) {
    
    if(!ColocationHelper.isColocationComplete(this.partitionedRegion)) {
      ProxyBucketRegion pb = this.partitionedRegion.getRegionAdvisor().getProxyBucketArray()[bucketId];
      BucketPersistenceAdvisor persistenceAdvisor = pb.getPersistenceAdvisor();
      
      //Don't worry about colocation if we're recovering a persistent
      //bucket. The bucket must have been properly colocated earlier.
      if(persistenceAdvisor != null && persistenceAdvisor.wasHosting()) {
        return true;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Colocation is incomplete");
      }
      return false;
    }
    return true;
  }

  

  /**
   * This method creates bucket regions, based on redundancy level. If
   * redundancy level is: a) = 1 it creates a local region b) >1 it creates a
   * distributed region
   * 
   * @param bucketId
   * @return @throws
   *         CacheException
   */
  private BucketRegion createBucketRegion(int bucketId)
  {
    this.partitionedRegion.checkReadiness();

    BucketAttributesFactory factory = new BucketAttributesFactory();
    
    boolean isPersistent = this.partitionedRegion.getDataPolicy().withPersistence();
    if(isPersistent) {
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      setDiskAttributes(factory);
    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    
    if(PartitionedRegion.DISABLE_SECONDARY_BUCKET_ACK) {
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    }
    else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
    }
    factory.setConcurrencyChecksEnabled(this.partitionedRegion.concurrencyChecksEnabled);
    factory.setIndexMaintenanceSynchronous(this.partitionedRegion.getIndexMaintenanceSynchronous());

    if(this.partitionedRegion.getValueConstraint() != null) {
      factory.setValueConstraint(this.partitionedRegion.getValueConstraint());
    }
    if (this.loader != null) {
      factory.setCacheLoader(this.loader);
    }
    factory.setEnableAsyncConflation(true);
    
    if (Boolean.getBoolean("gemfire.PRDebug")) {
      factory.addCacheListener(createDebugBucketListener());
    }
    
    if (this.partitionedRegion.getStatisticsEnabled()) {
      factory.setStatisticsEnabled(true);
    }

    factory.setCloningEnabled(this.partitionedRegion.getCloningEnabled());

    ExpirationAttributes ea = this.partitionedRegion.getAttributes().getEntryIdleTimeout();
    if (ea != null) {
      factory.setEntryIdleTimeout(ea);
    }
    ea = this.partitionedRegion.getAttributes().getEntryTimeToLive();
    if (ea != null) {
      factory.setEntryTimeToLive(ea);
    }
    ea = this.partitionedRegion.getAttributes().getRegionIdleTimeout();
    if (ea != null) {
      if(ea.getAction() != ExpirationAction.DESTROY)
        factory.setRegionIdleTimeout(ea);
    }
    ea = this.partitionedRegion.getAttributes().getRegionTimeToLive();
    if (ea != null) {
      if(ea.getAction() != ExpirationAction.DESTROY)
        factory.setRegionTimeToLive(ea);
    }
    CustomExpiry  ce = this.partitionedRegion.getAttributes().getCustomEntryIdleTimeout();
    if (ce != null) {
      factory.setCustomEntryIdleTimeout(ce);
    }
    ce = this.partitionedRegion.getAttributes().getCustomEntryTimeToLive();
    if (ce != null) {
      factory.setCustomEntryTimeToLive(ce);
    }

    if (this.partitionedRegion.getStatisticsEnabled()) { 
      factory.setStatisticsEnabled(true); 
    }
    EvictionAttributesImpl eva = (EvictionAttributesImpl)this.partitionedRegion.getEvictionAttributes();
    if (eva != null) {
      EvictionAttributes evBucket = eva;
      factory.setEvictionAttributes(evBucket);
      if (evBucket.getAction().isOverflowToDisk()) {
    	setDiskAttributes(factory);
      }
    }
    
    factory.setCompressor(this.partitionedRegion.getCompressor());
    factory.setOffHeap(this.partitionedRegion.getOffHeap());
    
    factory.setBucketRegion(true); // prevent validation problems
    RegionAttributes attributes = factory.create();
    
    final String bucketRegionName = this.partitionedRegion.getBucketName(bucketId);

    LocalRegion rootRegion = PartitionedRegionHelper.getPRRoot(this.partitionedRegion.getCache());
    BucketRegion bucketRegion = null;

    if (Boolean.getBoolean("gemfire.PRDebug")) {
      logger.info(LocalizedMessage.create(
          LocalizedStrings.PartitionedRegionDataStore_CREATEBUCKETREGION_CREATING_BUCKETID_0_NAME_1,
          new Object[] {this.partitionedRegion.bucketStringForLogs(bucketId), bucketRegionName}));
    }
    try {
      final Bucket proxyBucket = this.partitionedRegion.getRegionAdvisor().getBucket(bucketId);
      bucketRegion = (BucketRegion)rootRegion.createSubregion(bucketRegionName,
          attributes, new InternalRegionArguments()
          .setPartitionedRegionBucketRedundancy(this.partitionedRegion.getRedundantCopies())
          .setBucketAdvisor(proxyBucket.getBucketAdvisor())
          .setPersistenceAdvisor(proxyBucket.getPersistenceAdvisor())
          .setDiskRegion(proxyBucket.getDiskRegion())
          .setCachePerfStatsHolder(this)
          .setLoaderHelperFactory(this.partitionedRegion)
          .setPartitionedRegion(this.partitionedRegion)
          .setIndexes(getIndexes(rootRegion.getFullPath(), bucketRegionName)));
      this.partitionedRegion.getPrStats().incBucketCount(1);
    }
    catch (RegionExistsException ex) {
      // Bucket Region is already created, so do nothing.
      if (logger.isDebugEnabled()) {
        logger.debug("PartitionedRegionDataStore#createBucketRegion: Bucket region already created for bucketId={}{}{}",
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, ex);
      }
      bucketRegion = (BucketRegion)rootRegion.getSubregion(bucketRegionName);
    }
    catch (IOException ieo) {
      logger.debug("Unexpected error creating bucket in region", ieo);
      Assert.assertTrue(false, "IOException creating bucket Region: " + ieo);
    }
    catch (ClassNotFoundException cne) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unexpected error creating bucket in region", cne);
      }
      Assert.assertTrue(false,
          "ClassNotFoundException creating bucket Region: " + cne);
    }catch (InternalGemFireError e) {
      if (logger.isDebugEnabled()) {
        logger.info(LocalizedMessage.create(LocalizedStrings.PartitionedRegionDataStore_ASSERTION_ERROR_CREATING_BUCKET_IN_REGION), e);
      }
      this.getPartitionedRegion().checkReadiness();
      throw e;
    } 
    // Determine the size of the bucket (the Region in this case is mirrored,
    // get initial image has populated the bucket, compute the size of the
    // region)
    if (Boolean.getBoolean("gemfire.PRDebug")) {
      dumpBuckets(); 
      dumpBucket(bucketId, bucketRegion);
    }
    
//      Iterator i = localRegion.entrySet().iterator();
//      while (i.hasNext()) {
//        try {
//          NonTXEntry nte = (NonTXEntry) i.next();
//          // updateBucket2Size(bucketId.longValue(), localRegion, null);
//          // nte.getRegionEntry().getValueInVM();
//        } catch (EntryDestroyedException ignore) {}
//      }

    return bucketRegion;
  }

  private List getIndexes(String rootRegion, String bucketRegion) {
    List indexes = null;
    if (!this.partitionedRegion.isIndexed()) {
      return indexes;
    }

    // Get PR indexes.
    Map indexMap = this.partitionedRegion.getIndex();
    if (indexMap == null || indexMap.isEmpty()) {
      return indexes;
    }

    // Build index info thats used to create indexes on bucket regions.
    indexes = new ArrayList();
    Set indexSet = indexMap.entrySet();
    for (Iterator it = indexSet.iterator(); it.hasNext();) {
      try {
        Map.Entry indexEntry = (Map.Entry)it.next();
        PartitionedIndex index = (PartitionedIndex)indexEntry.getValue();
        IndexCreationData icd = new IndexCreationData(index.getName());        
        new QCompiler();
        String imports = index.getImports();
        icd.setIndexData(index.getType(), index.getCanonicalizedFromClause(), 
            index.getCanonicalizedIndexedExpression(), index.getImports());
        icd.setPartitionedIndex(index);
        indexes.add(icd);
      } catch (Exception ignor) {
        // since bucket creation should not fail.
        logger.info(LocalizedMessage.create(LocalizedStrings.PartitionedRegionDataStore_EXCPETION__IN_BUCKET_INDEX_CREATION_, 
            ignor.getLocalizedMessage()), ignor);
      }
    }
    return indexes;
  }

  
  private void setDiskAttributes(BucketAttributesFactory factory) {
    factory.setDiskSynchronous(this.partitionedRegion.getAttributes().isDiskSynchronous());
    factory.setDiskStoreName(this.partitionedRegion.getAttributes().getDiskStoreName());
  }
  
  public void assignBucketRegion(int bucketId, BucketRegion bukReg) {
    final Object oldbukReg = this.localBucket2RegionMap.putIfAbsent(Integer.valueOf(bucketId), bukReg);
    if (logger.isDebugEnabled()) {
      logger.debug("assigning bucket {} old assignment: {}", bukReg, oldbukReg);
    }
    Assert.assertTrue(oldbukReg==null);
  }
  
 /*public void removeBucketRegion(int bucketId) {
    Assert.assertHoldsLock(this.bucketAdminLock,true);    
    this.localBucket2RegionMap.remove(Long.valueOf(bucketId));    
  }*/

  private CacheListener createDebugBucketListener()
  {
    return new CacheListener() {
      public void afterCreate(EntryEvent event)
      {
        EntryEventImpl ee = (EntryEventImpl)event;
        logger.debug("BucketListener: o={}, r={}, k={}, nv={}, dm={}", event.getOperation(), event.getRegion().getFullPath(),
            event.getKey(), ee.getRawNewValue(), event.getDistributedMember());
      }

      public void afterUpdate(EntryEvent event)
      {
        EntryEventImpl ee = (EntryEventImpl)event;
        logger.debug("BucketListener: o={}, r={}, k={}, ov={}, nv={}, dm={}", event.getOperation(), event.getRegion().getFullPath(),
            event.getKey(), ee.getRawOldValue(), ee.getRawNewValue(), event.getDistributedMember());
      }

      public void afterInvalidate(EntryEvent event)
      {
        logger.debug("BucketListener: o={}, r={}, k={}, dm={}", event.getOperation(), event.getRegion().getFullPath(),
            event.getKey(), event.getDistributedMember());
      }

      public void afterDestroy(EntryEvent event)
      {
        logger.debug("BucketListener: o={}, r={}, k={}, dm={}", event.getOperation(), event.getRegion().getFullPath(),
            event.getKey(), event.getDistributedMember());
      }
      public final void afterRegionInvalidate(RegionEvent event) {}
      public final void afterRegionDestroy(RegionEvent event) {}
      public final void afterRegionClear(RegionEvent event) {}
      public final void afterRegionCreate(RegionEvent event) {}
      public final void afterRegionLive(RegionEvent event) {}
      public final void close() {}
    };
  }

//  private void addBucketMapping(Integer bucketId, Node theNode)
//  {
//    VersionedArrayList list = (VersionedArrayList)this.partitionedRegion
//        .getBucket2Node().get(bucketId);
//    // Create a new list to avoid concurrent modification exceptions when
//    // the array list is serialized e.g. GII
//    if (list == null) {
//      list = new VersionedArrayList(
//          this.partitionedRegion.getRedundantCopies() + 1);
//      list.add(theNode);
//
//    }
//    else {
//      for(Iterator itr =list.iterator(); itr.hasNext();) {
//       	Node nd = (Node)itr.next();
//       	if( !PartitionedRegionHelper.isMemberAlive(nd.getMemberId(), 
//            this.partitionedRegion.cache) 
//       	    && !this.partitionedRegion.isPresentInPRConfig(nd)) {       		      	 
//       		list.remove(nd);
//       	  if(list.size() ==0 ) {
//       	    PartitionedRegionHelper.logForDataLoss(this.partitionedRegion, 
//                bucketId.intValue(), "addBucketMapping");
//          }
//       	}
//       	
//      }
//      if (!list.contains(theNode)) {
//        list.add(theNode);
//      }
//    }
//    this.partitionedRegion.checkClosed();
//    this.partitionedRegion.checkReadiness();     
//    this.partitionedRegion.getBucket2Node().put(bucketId, list);    
//  }

  public CacheLoader getCacheLoader()
  {
    return this.loader;
  }

  /**
   * sent by the partitioned region when its loader has changed
   */
  protected void cacheLoaderChanged(final CacheLoader newLoader, final CacheLoader oldLoader) {
    StoppableWriteLock lock = this.bucketCreationLock.writeLock();
    lock.lock();
    try {
      this.loader = newLoader;
      visitBuckets(new BucketVisitor() {
        @Override
        public void visit(Integer bucketId, Region r) {
          AttributesMutator mut = r.getAttributesMutator();
          if (logger.isDebugEnabled()) {
            logger.debug("setting new cache loader in bucket region: {}", newLoader);
          }
          mut.setCacheLoader(newLoader);
        }
      });
    } finally {
      lock.unlock();
    }
  }
  
  /**
   * Gets the total amount of memory in bytes allocated for all values for this
   * PR in this VM. This is the current memory (MB) watermark for data in this
   * PR.
   * 
   * If eviction to disk is enabled, this does not reflect the size
   * of entries on disk.
   * 
   * @return the total memory size in bytes for all the Map's values
   */
  public long currentAllocatedMemory()
  {
    return this.bytesInUse.get();
  }

  /**
   * Checks if this PartitionedRegionDataStore has the capacity to handle the
   * bucket creation request. If so, creates the real storage for the bucket.
   * 
   * @param bucketId
   *          the bucket id
   * @param size
   *          the size in bytes of the bucket to create locally
   * @param forceCreation ignore local maximum buckets check  
   * @return true if already managing the bucket or if the bucket
   *         has been created
   */
  public boolean handleManageBucketRequest(int bucketId, 
                                           int size, 
                                           InternalDistributedMember sender, 
                                           boolean forceCreation) {
    
    // check maxMemory setting
    if (!this.partitionedRegion.isDataStore()) {
      if (logger.isDebugEnabled()) {
        logger.debug("handleRemoteManageBucket: local max memory is zero");
      }
      return false;
    }
    if (! canAccommodateMoreBytesSafely(size)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Partitioned Region {} has exceeded local maximum memory configuration {} Mb, current size is {} Mb",
            this.partitionedRegion.getFullPath(), this.partitionedRegion.getLocalMaxMemory(),
            (this.bytesInUse.get() / PartitionedRegionHelper.BYTES_PER_MB));

        logger.debug("Refusing remote bucket creation request for bucketId={}{}{} of size {} Mb.",
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId,
            (size / PartitionedRegionHelper.BYTES_PER_MB));
      }
      return false;
    }
    
    if (! forceCreation && ! canAccommodateAnotherBucket()) {
      return false;
    }

    boolean createdBucket = false;
    if(grabBucket(bucketId, null, forceCreation, false, true, sender, false).nowExists()) {
      this.partitionedRegion.checkReadiness();
      if (logger.isDebugEnabled()) {
        logger.debug("handleManageBucketRequest: successful, returning:{} bucketId={}{}{} for PR = {}", this.partitionedRegion.getMyId(),
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, this.getName());
      }
      createdBucket = true;
    }
    else {
      // somebody else got it already
      if (logger.isDebugEnabled()) {
        logger.debug("handleManageBucketRequest: someone else grabbed this bucket");
      }
    }
   
    return createdBucket;
  }

  /**
   * Determine if the ratio of buckets this VM should host is appropriate 
   * given its localMaxMemory setting as compared to others
   * 
   * @return true if this data store can host another bucket
   */
  boolean canAccommodateAnotherBucket() {
    final int localMax = this.partitionedRegion.getLocalMaxMemory(); 

    double totalMax = (double)this.partitionedRegion.getRegionAdvisor()
          .adviseTotalMemoryAllocation()
          + localMax;
    Assert.assertTrue(totalMax > 0.0);

    final double memoryRatio = localMax / totalMax;
    
    Assert.assertTrue(memoryRatio > 0.0);
    Assert.assertTrue(memoryRatio <= 1.0);
    
    final int totalBucketInstances = this.partitionedRegion.getTotalNumberOfBuckets() 
      * (this.partitionedRegion.getRedundantCopies() + 1);
    
    final double numBucketsToHostLocally = Math.ceil(memoryRatio 
        * totalBucketInstances);
    
    Assert.assertTrue(numBucketsToHostLocally > 0.0);

    //Pessimistically assume that all concurrent bucket creates will succeed.
    //-1 because we've already incremented bucketCreatesInProgress to include this thread.
    final int currentNumBuckets = this.localBucket2RegionMap.size() + bucketCreatesInProgress.intValue() - 1;
    boolean ret = numBucketsToHostLocally > currentNumBuckets;
    if (logger.isDebugEnabled()) {
      logger.debug("canAccomodateAnotherBucket: local VM can host {} does host {} concurrent creates {}",
          numBucketsToHostLocally, this.localBucket2RegionMap.size(), (bucketCreatesInProgress.intValue() - 1));
    }
    
    if (! ret) {
      // TODO make this an info message when bucket creation requests
      // arrive in a proportional fashion e.g. if a VM's localMaxMemory is 1/2 of it's
      // peer, it should receive half as many bucket creation requests
      if (logger.isDebugEnabled()) {
        logger.debug("Partitioned Region {} potentially unbalanced; maximum number of buckets, {}, has been reached",
            this.partitionedRegion.getFullPath(), numBucketsToHostLocally);
        logger.debug("Total max: {} memoryRatio: {}", totalMax, memoryRatio);
      }
    }
    
    return ret;
  }
  
  /**
   * Checks if this PartitionedRegionDataStore has the capacity to handle the
   * rebalancing size.
   * 
   * @param size
   *          the size in bytes of the bucket to be rebalanced
   * @return true if size can be accommodated without exceeding ratioFull
   */
  boolean handleRemoteCanRebalance(long size)
  {
    return false;
  }

  // /////////////////////////////////////
  // /////////// Empty methods //////////
  // ////////////////////////////////////

  /**
   * Handles rebalance by accepting new bucket contents and storing it.
   * 
   * @param bucketId
   *          the id of the bucket to rebalance
   * @param obj
   *          the contents of the bucket
   * @param regionName
   *          the name of the PR
   * @return true if successful
   */
  boolean handleRemoteRebalance(int bucketId, Object obj, String regionName)
  {
    return false;
  }

  /**
   * Checks if this PR has the capacity to handle the rebalancing size. If so,
   * creates the real storage for the bucket and a bucket2Node Region mapping.
   * These two operations are done as a logical unit so that the node can
   * immediately begin handling remote requests once the bucket2Node mapping
   * becomes visible.
   * 
   * @param bucketId
   *          the bucket id
   */
  boolean handleRemoteCreateBackupRegion(int bucketId)
  {
    return false;
  }

  /**
   * Return the size in bytes for a given bucket.
   * 
   * @param bucketId
   * @return size in bytes
   */
  public long getBucketSize(int bucketId)
  {
    // This is an approximate calculation, so we don't require the
    // bucket to be fully initialized...
    // getInitializedBucketForId(Long.valueOf(bucketId)); // wait for the bucket to finish initializing
    final BucketRegion bucketRegion = this.localBucket2RegionMap
        .get(Integer.valueOf(bucketId));
    if(bucketRegion == null) {
      return 0;
    }
    return bucketRegion.getTotalBytes();
  }
  
  /**
   * Querys the buckets in this data store for query specified by queryPredicate.
   * @param query
   * @param parameters the parameters to be used to execute the query
   * @param buckets to be queried
   * @param resultCollector
   * @throws QueryException TODO-javadocs
   */
  /*
  public void queryLocalNode(DefaultQuery query, Object[] parameters, List buckets, PRQueryResultCollector resultCollector)
    throws QueryException, InterruptedException
  {
    Assert.assertTrue(!buckets.isEmpty(), "bucket list can not be empty. ");
    invokeBucketReadHook();

    // Check if QueryMonitor is enabled, if so add query to be monitored.
    QueryMonitor queryMonitor = null;
    if( GemFireCacheImpl.getInstance() != null)
    {
      queryMonitor = GemFireCacheImpl.getInstance().getQueryMonitor();
    }
    
    try {
      if (queryMonitor != null) {
        // Add current thread to be monitored by QueryMonitor.
        queryMonitor.monitorQueryThread(Thread.currentThread(), query);
      }
      new PRQueryProcessor(this, query, parameters, buckets).executeQuery(resultCollector);
    } finally {
      if (queryMonitor != null) {
        queryMonitor.stopMonitoringQueryThread(Thread.currentThread(), query);
      }
    }
  }
  */

  /**
   * This method returns name of this Partitioned Region
   * 
   * @return Partitioned Region name
   */
  private String getName()
  {
    return this.partitionedRegion.getName();
  }

  


  // /////////////////////////////////////
  // /////////// Local put //////////////
  // ////////////////////////////////////

  /**
   * Puts the object with the given key locally. <br>
   * Step: <br>
   * 1) It finds out the bucket region for the bucket id. <br>
   * 2) If from step 1 it gets null, that means the bucket is re-mapped. <br>
   * 3) If it finds the bucket region from step 1, it tries to put the key-value
   * on the region. <br>
   * 4) updateBucket2Size if bucket is on more than 1 node or else bucket
   * listeners would take care of size update. <br>
   * 
   * @param bucketId the bucket id of the key
   * @param event the operation event
   * @param ifNew whether a create must be performed
   * @param ifOld whether an existing entry must be updated
   * @param lastModified time stamp for update operations
   * @throws ForceReattemptException
   *           if bucket region is null
   * @throws PrimaryBucketException if the bucket in this data store is not the primary bucket
   * @return true if put happened
   */
  public boolean putLocally(final Integer bucketId,
                            final EntryEventImpl event,
                            boolean ifNew,
                            boolean ifOld,
                            Object expectedOldValue,
                            boolean requireOldValue,
                            final long lastModified)
  throws PrimaryBucketException, ForceReattemptException {
    final BucketRegion br = getInitializedBucketForId(event.getKey(), bucketId);
    return putLocally(br, event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified);
  }

  public boolean putLocally(final BucketRegion bucketRegion,
                            final EntryEventImpl event,
                            boolean ifNew,
                            boolean ifOld,
                            Object expectedOldValue,
                            boolean requireOldValue,
                            final long lastModified)
  throws PrimaryBucketException, ForceReattemptException {
    boolean didPut=false; // false if entry put fails
    
//    final BucketRegion bucketRegion = getInitializedBucketForId(event.getKey(), bucketId);
    
    try{
      event.setRegion(bucketRegion);

      if (event.isOriginRemote()) {
        didPut = bucketRegion.basicUpdate(event, ifNew, ifOld, lastModified,
            false);
      }
      else {
        // Skip yet another validation
        didPut = bucketRegion.virtualPut(event,
                                         ifNew,
                                         ifOld,
                                         expectedOldValue,
                                         requireOldValue,
                                         lastModified,
                                         false);
      }
      // bug 34361: don't send a reply if bucket was destroyed during the op
      bucketRegion.checkReadiness();
    }
    catch(RegionDestroyedException rde){
      checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(), rde);
    } 
    
    return didPut;
  }
  
  protected boolean hasClientInterest(EntryEventImpl event) {
    return UPDATE_ACCESS_TIME_ON_INTEREST
        && this.keysOfInterest.containsKey(event.getKey());
  }
  
  
    
  protected void updateMemoryStats(final long memoryDelta) {
    // Update stats
    this.partitionedRegion.getPrStats().incBytesInUse(memoryDelta);

    final long locBytes = this.bytesInUse.addAndGet(memoryDelta);

    if(!this.partitionedRegion.isEntryEvictionPossible()) {
      StringId logStr = null;
      //only check for exceeding local max memory if we're not evicting entries.
      // TODO, investigate precision issues with cast to long 
      if (!this.exceededLocalMaxMemoryLimit) { // previously OK
        if (locBytes > this.maximumLocalBytes) { // not OK now
          this.exceededLocalMaxMemoryLimit = true;
          logStr = LocalizedStrings.PartitionedRegionDataStore_PARTITIONED_REGION_0_HAS_EXCEEDED_LOCAL_MAXIMUM_MEMORY_CONFIGURATION_2_MB_CURRENT_SIZE_IS_3_MB;
        }
      }
      else {
        if (locBytes <= this.maximumLocalBytes) {  
          this.exceededLocalMaxMemoryLimit = false;
          logStr = LocalizedStrings.PartitionedRegionDataStore_PARTITIONED_REGION_0_IS_AT_OR_BELOW_LOCAL_MAXIMUM_MEMORY_CONFIGURATION_2_MB_CURRENT_SIZE_IS_3_MB;  
        }
      }
      if (logStr != null) {
        Object[] logArgs = new Object[] {this.partitionedRegion.getFullPath(), logStr, Long.valueOf(this.partitionedRegion.getLocalMaxMemory()), Long.valueOf(locBytes / PartitionedRegionHelper.BYTES_PER_MB)}; 
        if (this.exceededLocalMaxMemoryLimit) {
          logger.warn(LocalizedMessage.create(logStr, logArgs));
        } else {
          logger.info(LocalizedMessage.create(logStr, logArgs));
        }
      }
    }
  }

  /**
   * Checks whether there is room in this Map to accommodate more data.
   * 
   * @param bytes
   *          the size to check in bytes
   */
  boolean canAccommodateMoreBytesSafely(int bytes)
  {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (this.partitionedRegion.getLocalMaxMemory() == 0) {
      return false;
    }
    if(this.partitionedRegion.isEntryEvictionPossible()) {
      //assume that since we're evicting entries, we're allowed to go over localMaxMemory and
      //eviction will take care of keeping us from running out of memory.
      return true;
    }
    // long allocatedMemory = currentAllocatedMemory();
    // precision coercion from int to long on bytes
    final long curBytes = this.bytesInUse.get();
    if (isDebugEnabled) {
      logger.debug("canAccomodateMoreBytes: bytes = {} allocatedMemory = {} newAllocatedSize = {} thresholdSize = ",
          bytes, curBytes, (curBytes+bytes), this.maximumLocalBytes);
    }
    if ((curBytes + bytes) < this.maximumLocalBytes) {
      if (isDebugEnabled) {
        logger.debug("canAccomodateMoreBytes: returns true");
      }
      return true;
    }
    else {
      if (isDebugEnabled) {
        logger.debug("canAccomodateMoreBytes: returns false");
      }
      return false;
    }
  }

  // static void update
  /**
   * Returns the PartitionRegion of Data store.
   */
  public PartitionedRegion getPartitionedRegion()
  {
    return this.partitionedRegion;
  }

  /**
   * Handles the remote request to remove the key from this Map. <br>
   * Step: <br>
   * 1) Locates the bucket region. If it doesnt find the actual bucket, it means
   * that this bucket is remapped to some other node and remappedBucketException
   * is thrown <br>
   * 2) Invokes destroy on that bucket region <br>
   * 3) updateBucket2Size if bucket is on more than 1 node or else bucket
   * listners would take care of size update.
   * 
   * @param bucketId
   *          for the key
   * @param event the event causing this action
   * @param expectedOldValue if non-null, then only succeed if current value
   * @return the removed value
   * @throws EntryNotFoundException
   *           if entry is not found for the key or expectedOldValue is
   *           not null and current value is not equal to it
   * @throws PrimaryBucketException if the locally managed bucket is not the primary
   * @throws ForceReattemptException if the bucket region is null
   */
  public Object destroyLocally(Integer bucketId,
                               EntryEventImpl event,
                               Object expectedOldValue)
  throws EntryNotFoundException,
         PrimaryBucketException,
         ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug("destroyLocally: key={} bucketId={}{}{}", event.getKey(),
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId); 
    }
    Object obj = null;
    final BucketRegion bucketRegion = getInitializedBucketForId(event.getKey(), bucketId);
    try {
      event.setRegion(bucketRegion);

      // ?? :ezoerner:20080815 the reason why the old value used to be set here
      // early (before sync on RegionEntry) is unknown.
      // However, it is necessary to set it "early" in the case of
      // distribution without destroying locally (hasSeenEvent), but this is
      // unnecessary (and wrong) otherwise.
      // Setting the value is deferred until just before
      // distribution in BucketRegion#basicDestroy
      // @see BucketRegion#basicDestroy
      // event.setOldValueFromRegion();

      bucketRegion.basicDestroy(event, true, expectedOldValue);
      // bug 34361: don't send a reply if bucket was destroyed during the op
      bucketRegion.checkReadiness();

    }
    catch (EntryNotFoundException enf) {
      if (this.partitionedRegion.isDestroyed()) {
        checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(),
            new RegionDestroyedException(LocalizedStrings.PartitionedRegionDataStore_REGION_HAS_BEEN_DESTROYED.toLocalizedString(), this.partitionedRegion.getFullPath()));
      }

      // ???:ezoerner:20080815 why throw a new exception here and lose the
      // stack trace when he had a perfectly good one already?
      // throw new EntryNotFoundException("Entry not found for key = " +
      //                                   event.getKey());

      throw enf; // rethrow
    }
    catch(RegionDestroyedException rde){
      checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(), rde);
    }

    // this is done elsewhere now
    //event.setRegion(this.partitionedRegion);
    //this.partitionedRegion.notifyBridgeClients(EnumListenerEvent.AFTER_DESTROY,
    //    event);
    //this.partitionedRegion.notifyGatewayHub(EnumListenerEvent.AFTER_DESTROY,
    //    event);

    return obj;
  }

  /**
 * This method does the cleaning up of the closed/locallyDestroyed PartitionedRegion.
 * This cleans up the reference of the close PartitionedRegion's node from the b2n region and locallyDestroys the b2n region (if removeBucketMapping is true).
 * It locallyDestroys the bucket region and cleans up the localBucketRegion map to avoid any stale references to locally destroyed bucket region. 
 * @param removeBucketMapping
 */
  void cleanUp(boolean removeBucketMapping, boolean removeFromDisk)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("cleanUp: Starting cleanup for {}", this.partitionedRegion);
    }
    try {
      if (removeBucketMapping) {
        if (logger.isDebugEnabled()) {
          logger.debug("cleanUp: Done destroyBucket2NodeRegionLocally for {}", this.partitionedRegion);
        }
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("cleanUp: not removing node from b2n region");
        }
      }
      
      // Lock out bucket creation while doing this :-)
      StoppableWriteLock lock = this.bucketCreationLock.writeLock();
      lock.lock();
      try {
        ProxyBucketRegion[] proxyBuckets = getPartitionedRegion().getRegionAdvisor().getProxyBucketArray();
        if(proxyBuckets != null) {
          for(ProxyBucketRegion pbr : proxyBuckets) {
            Integer bucketId = Integer.valueOf(pbr.getBucketId());
            BucketRegion buk = localBucket2RegionMap.get(bucketId);
            // concurrent entry iterator does not guarantee value, key pairs  
            if (buk != null) { 
              try {
                buk.getBucketAdvisor().getProxyBucketRegion().setHosting(false);
                if(removeFromDisk) {
                  buk.localDestroyRegion();
                } else {
                  buk.close();
                }
                if (logger.isDebugEnabled()) {
                  logger.debug("cleanup: Locally destroyed bucket {}", buk.getFullPath());
                }
                //Fix for defect #49012
                if (buk instanceof AbstractBucketRegionQueue
                    && buk.getPartitionedRegion().isShadowPR()) {
                  if (buk.getPartitionedRegion().getColocatedWithRegion() != null) {
                    buk.getPartitionedRegion().getColocatedWithRegion()
                        .getRegionAdvisor().getBucketAdvisor(bucketId)
                        .setShadowBucketDestroyed(true);
                  }
                }
              }
              catch (RegionDestroyedException ignore) {
              }
              catch (Exception ex) {
                logger.warn(LocalizedMessage.create(
                    LocalizedStrings.PartitionedRegion_PARTITIONEDREGION_0_CLEANUP_PROBLEM_DESTROYING_BUCKET_1,
                    new Object[] {this.partitionedRegion.getFullPath(), Integer.valueOf(buk.getId())}), ex);
              }

              localBucket2RegionMap.remove(bucketId);
            } else if (removeFromDisk) {
              DiskRegion diskRegion = pbr.getDiskRegion();
              if(diskRegion != null) {
                diskRegion.beginDestroy(null);
                diskRegion.endDestroy(null);
              }
            }
          } // while
        }
      }
      finally {
        lock.unlock();
      }
    }
    catch (Exception ex) {
      logger.warn(LocalizedMessage.create(
        LocalizedStrings.PartitionedRegionDataStore_PARTITIONEDREGION_0_CAUGHT_UNEXPECTED_EXCEPTION_DURING_CLEANUP,
        this.partitionedRegion.getFullPath()), ex);
    }
    finally {
      this.partitionedRegion.getPrStats().setBucketCount(0);
      this.bucketStats.close();
    }
  }

  /**
   * Removes a redundant bucket hosted by this data store. The rebalancer
   * invokes this method directly or sends this member a message to invoke it.
   * 
   * From the spec:
   * 
   * How to Remove a Redundant Bucket
   * 
   * This operation is done by the rebalancer (REB) and can only be done on
   * non-primary buckets. If you want to remove a primary bucket first send
   * one of its peers "become primary" and then send it "unhost" (we could
   * offer a "unhost" option on "become primary" or a "becomePrimary" option
   * on "create redundant"). The member that has the bucket being removed is
   * called the bucket host (BH).
   * 
   * 1. REB sends an "unhostBucket" message to BH. This message will be
   *    rejected if the member finds itself to be the primary or if he doesn't
   *    host the bucket by sending a failure reply to REB.
   * 2. BH marks itself as "not-hosting". This causes any read operations that
   *    come in to not start and retry. BH also updates the advisor to know
   *    that it is no longer hosting the bucket.
   * 3. BH then waits for any in-progress reads (which read ops to wait for
   *    are TBD) to complete.
   * 4. BH then removes the bucket region from its cache.
   * 5. BH then sends a success reply to REB.
   * 
   * This method is now also used by the PartitionManager.
   * For the PartitionManager, it does remove the primary bucket.
   * 
   * @param bucketId the id of the bucket to remove
   * 
   * @return true if the bucket was removed; false if unable to remove or if
   * bucket is not hosted
   */
  public boolean removeBucket(int bucketId, boolean forceRemovePrimary) {
    waitForInProgressBackup();
    
    //Don't allow the removal of a bucket if we haven't
    //finished recovering from disk
    if(!this.partitionedRegion.getRedundancyProvider().isPersistentRecoveryComplete()) {
      if (logger.isDebugEnabled()) {
       logger.debug(
                "Returning false from removeBucket because we have not finished recovering all colocated regions from disk");
      }
      return false;
    }
    // Lock out bucket creation while doing this :-)
    StoppableWriteLock lock = this.bucketCreationLock.writeLock();
    lock.lock();
    try {
      BucketRegion bucketRegion = this.localBucket2RegionMap.get(Integer
          .valueOf(bucketId));
      if (bucketRegion == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Returning true from removeBucket because we don't have the bucket we've been told to remove");
        }
        return true;

      }

      BucketAdvisor bucketAdvisor = bucketRegion.getBucketAdvisor();
      Lock writeLock = bucketAdvisor.getActiveWriteLock();

      // Fix for 43613 - don't remove the bucket
      // if we are primary. We hold the lock here
      // to prevent this member from becoming primary until this
      // member is no longer hosting the bucket.
      writeLock.lock();
      try {
        if (!forceRemovePrimary && bucketAdvisor.isPrimary()) {
          return false;
        }
        
        // recurse down to each tier of children to remove first
        removeBucketForColocatedChildren(bucketId, forceRemovePrimary);

        if (bucketRegion.getPartitionedRegion().isShadowPR()) {
          if (bucketRegion.getPartitionedRegion().getColocatedWithRegion() != null) {
            bucketRegion.getPartitionedRegion().getColocatedWithRegion()
                .getRegionAdvisor().getBucketAdvisor(bucketId)
                .setShadowBucketDestroyed(true);
          }
        }
        bucketAdvisor.getProxyBucketRegion().removeBucket();
      }
      finally {
        writeLock.unlock();
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Removed bucket {} from advisor", bucketRegion);
      }

      // Flush the state of the primary. This make sure we have processed
      // all operations were sent out before we removed our profile from
      // our peers.
      //
      // Another option, instead of using the StateFlushOperation, could
      // be to send a message which waits until it acquires the
      // activePrimaryMoveLock on primary the bucket region. That would also
      // wait for in progress writes. I choose to use the StateFlushOperation
      // because it won't block write operations while we're trying to acquire
      // the activePrimaryMoveLock
      InternalDistributedMember primary = bucketAdvisor.getPrimary();
      InternalDistributedMember myId = this.partitionedRegion
          .getDistributionManager().getDistributionManagerId();
      if (!myId.equals(primary)) {
        StateFlushOperation flush = new StateFlushOperation(bucketRegion);
        int executor = DistributionManager.WAITING_POOL_EXECUTOR;
        try {
          flush.flush(Collections.singleton(primary), myId, executor, false);
        }
        catch (InterruptedException e) {
          this.partitionedRegion.getCancelCriterion().checkCancelInProgress(e);
          Thread.currentThread().interrupt();
          throw new InternalGemFireException("Interrupted while flushing state");
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Finished state flush for removal of {}", bucketRegion);
        }
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("We became primary while destroying the bucket. Too late to stop now.");
        }
      }
      bucketRegion.invokePartitionListenerAfterBucketRemoved();
      bucketRegion.preDestroyBucket(bucketId);
      bucketRegion.localDestroyRegion();
      bucketAdvisor.getProxyBucketRegion().finishRemoveBucket();
      if (logger.isDebugEnabled()) {
        logger.debug("Destroyed {}", bucketRegion);
      }
      this.localBucket2RegionMap.remove(Integer.valueOf(bucketId));
      this.partitionedRegion.getPrStats().incBucketCount(-1);
      return true;
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * Wait for in an progress backup. When we backup the whole DS, we need to make 
   * sure we don't miss a bucket because it is in the process
   * of rebalancing. This doesn't wait for the whole backup to complete,
   * it only makes sure that this destroy will wait until the point
   * when we know that we that this bucket won't be destroyed on this member
   * in the backup unless it was backed up on the target member. 
   */
  private void waitForInProgressBackup() {
    BackupManager backupManager = getPartitionedRegion().getGemFireCache().getBackupManager();
    if(getPartitionedRegion().getDataPolicy().withPersistence() && backupManager != null) {
      backupManager.waitForBackup();
    }
    
  }

  /**
   * This calls removeBucket on every colocated child that is directly
   * colocated to this bucket's PR. Those each in turn do the same to their
   * child buckets and so on before returning.
   *
   * @param bucketId the bucket to remove
   * @param forceRemovePrimary true if we should remove the bucket, even if
   * it is primary.
   *
   * @return true if bucket was removed from all children
   */
  private boolean removeBucketForColocatedChildren(int bucketId, 
      boolean forceRemovePrimary) {
    boolean removedChildBucket = true;
    
    // getColocatedChildRegions returns only the child PRs directly colocated
    // with thisPR...
    List<PartitionedRegion> colocatedChildPRs = 
        ColocationHelper.getColocatedChildRegions(this.partitionedRegion);
    if (colocatedChildPRs != null) {
      for (PartitionedRegion pr : colocatedChildPRs) {
        removedChildBucket = 
            pr.getDataStore().removeBucket(bucketId, forceRemovePrimary) && removedChildBucket;
      }
    }
    
    return removedChildBucket;
  }
  
  /**
   * Create a new backup of the bucket, allowing redundancy to be exceeded.
   * All colocated child buckets will also be created.
   * 
   * @param bucketId the bucket to create
   * @param isRebalance true if bucket creation is directed by rebalancing
   * @return true if the bucket and its colocated chain of children are created
   */
  public CreateBucketResult createRedundantBucket(int bucketId, boolean isRebalance, InternalDistributedMember moveSource) {
    // recurse down to create each tier of children after creating leader bucket 
    return grabBucket(bucketId, moveSource, true, false, isRebalance, null, false);
  }
  
  /**
   * Moves the bucket from the <code>source</code> member to this datastore.
   * 
   * If the bucket is the leader bucket then it will recursively create all
   * colocated children and then remove all colocated children as well from
   * the <code>source</code> member.
   * 
   * @param bucketId the bucket to move
   * @param source the member to move the bucket from
   * @return true if bucket was successfully moved to this datastore
   */
  public boolean moveBucket(int bucketId, InternalDistributedMember source,
      final boolean isRebalance) {

    if (createRedundantBucket(bucketId, isRebalance, source) != CreateBucketResult.CREATED) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed to move bucket {} to {}", bucketId, this);
      }
      return false;
    }

    BucketAdvisor bucketAdvisor = this.partitionedRegion.getRegionAdvisor().getBucketAdvisor(bucketId);

    if (source.equals(bucketAdvisor.getPrimary())) {
      if (!bucketAdvisor.becomePrimary(true)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed to become primary for bucket {} on {}", bucketId, this);
        }
      }
    }

    RemoveBucketResponse response = RemoveBucketMessage.send(source,
        this.partitionedRegion, bucketId, false);
    if (response != null) {
      boolean removed = response.waitForResponse();
      if (removed == false) {
        if (logger.isDebugEnabled()) {
          logger.debug("Successfully created bucket {} in {} but failed to remove it from {}", bucketId, this, source);
        }
      }
      // TODO rebalance - perhaps we should thow an error if we
      // can't remove the bucket??
    }
    // The new bucket's size is counted in when GII
    return true;
  }
  
  /**
   * Fetch a BucketRegion, but do not return until it is initialized
   * @param key optional for error reporting; if none, no key available.
   * @param bucketId the bucket to  fetch
   * @return the region
   * @throws ForceReattemptException
   */
  public BucketRegion getInitializedBucketForId(Object key, Integer bucketId) 
      throws ForceReattemptException {
    final BucketRegion bucketRegion = this.localBucket2RegionMap.get(bucketId);
    if (null == bucketRegion) {
      this.partitionedRegion.checkReadiness();
      if (logger.isDebugEnabled()) {
        logger.debug("Got null bucket region for bucketId={}{}{} for PartitionedRegion = {}",
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, this.partitionedRegion);
      }
      ForceReattemptException fre = new BucketNotFoundException(LocalizedStrings.PartitionedRegionDataStore_BUCKET_ID_0_NOT_FOUND_ON_VM_1.toLocalizedString(new Object[] {this.partitionedRegion.bucketStringForLogs(bucketId.intValue()), this.partitionedRegion.getMyId()}));
      if (key != null) {
        fre.setHash(key.hashCode());
      }
      throw fre;
    }
    bucketRegion.waitForData();
    return bucketRegion;
  }

  /**
   * Returns the local BucketRegion given an bucketId.
   * Returns null if no BucketRegion exists.
   * @since 6.1.2.9
   */
  public BucketRegion getLocalBucketById(Integer bucketId) {
    final BucketRegion bucketRegion = this.localBucket2RegionMap.get(bucketId);
    if (bucketRegion != null) {
      bucketRegion.waitForData();
    }
    return bucketRegion;
  }
  
  
  /*
   * @return an initialized local bucket or null
   */
  public BucketRegion getLocalBucketByKey(Object key) {
    Integer bucketId = Integer.valueOf(PartitionedRegionHelper.getHashKey(this.partitionedRegion,
        null, key, null, null));
    return getLocalBucketById(bucketId);
  }

  /**
   * Test hook to return the per entry overhead for a bucket region.
   * PRECONDITION: a bucket must exist and be using LRU.
   * @since 6.1.2.9
   */
  public int getPerEntryLRUOverhead() {
    BucketRegion br = (localBucket2RegionMap.values().iterator().next());
    AbstractLRURegionMap map = (AbstractLRURegionMap)br.getRegionMap();
    return map.getEntryOverHead();
  }
  
  /**
   * Fetch a BucketRegion, but do not return until it is initialized
   * and the primary is known.
   * @see #getInitializedBucketForId(Object, Integer)
   * @param key
   * @param bucketId
   * @return the initialized region
   * @throws ForceReattemptException
   */
  public BucketRegion getInitializedBucketWithKnownPrimaryForId(Object key,
      Integer bucketId) throws ForceReattemptException {
    final BucketRegion br = getInitializedBucketForId(key, bucketId);
    br.getBucketAdvisor().getPrimary();// waits until the primary is initialized
    return br;
  }

  /**
   * Checks if this instance contains a value for the key locally.
   * 
   * @param bucketId
   *          for the key
   * @param key
   *          the key, whose value needs to be checks
   * @throws ForceReattemptException
   *           if bucket region is null
   * @return true if there is a non-null value for the given key
   * @throws PrimaryBucketException if the locally managed bucket is not the primary
   * @throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed 
   */
  public boolean containsValueForKeyLocally(Integer bucketId, Object key) 
      throws PrimaryBucketException, ForceReattemptException,
      PRLocallyDestroyedException 
  {
    final BucketRegion bucketRegion = getInitializedBucketForId(key, bucketId);
    invokeBucketReadHook();
    boolean ret=false;
    try {
      ret = bucketRegion.containsValueForKey(key);
      checkIfBucketMoved(bucketRegion);
    }
    catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during containsValueForKey", rde);
        } else {
          throw new RegionDestroyedException(LocalizedStrings.PartitionedRegionDataStore_UNABLE_TO_GET_VALUE_FOR_KEY.toLocalizedString(), this.partitionedRegion.toString(), rde);
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("containsValueForKeyLocally: key {} returning {}", key, ret);
    }

    return ret; 
  }

  /**
   * Throw a ForceReattemptException if bucket has been moved out of this data store.
   */
  private void checkIfBucketMoved(BucketRegion br) throws ForceReattemptException {
    if (br.isBucketDestroyed()) {
      this.partitionedRegion.checkReadiness();
      throw new ForceReattemptException("bucket moved to other member during read op");
    }
  }
  
  /**
   * Checks if this instance contains a key.
   * 
   * @param bucketId
   *          the bucketId for the key
   * @param key
   *          the key to look for
   * @throws ForceReattemptException
   *           if bucket region is null
   * @return true if there is an entry with the given key
   * @throws PrimaryBucketException if the bucket is not primary
   * @throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed 
   */
  public boolean containsKeyLocally(Integer bucketId, Object key) 
    throws PrimaryBucketException, ForceReattemptException,
           PRLocallyDestroyedException 
  { 
    final BucketRegion bucketRegion = getInitializedBucketForId(key, bucketId);
    invokeBucketReadHook();
    try {
      final boolean ret = bucketRegion.containsKey(key);
      checkIfBucketMoved(bucketRegion);
      if (logger.isDebugEnabled()) {
        logger.debug("containsKeyLocally:  key {}) bucketId={}{}{} region {} returns {}", key,
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, bucketRegion.getName(), ret);
      }
      return ret;
    } catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during containsKey", rde);
        } else {
          throw new RegionDestroyedException("Unable to do containsKey on",
                                             this.partitionedRegion.toString(), rde);
        }
      }
    }
  }

  /**
   * Test hook that will be invoked before any bucket read. It is invoked after
   * the bucket is acquired but before the bucket is locked and before the read
   * operation is done.
   */
  private Runnable bucketReadHook;
  /**
   * invokes a test hook, if it was installed, and removes it.
   */
  public void invokeBucketReadHook() {
    Runnable r = this.bucketReadHook;
    if (r != null) {
      setBucketReadHook(null);
      r.run();
    }
  }
  public void setBucketReadHook(Runnable r) {
    this.bucketReadHook = r;
  }
  
  /**
   * Returns value corresponding to this key.
   * @param key
   *          the key to look for
   * @param preferCD 
   * @param requestingClient the client making the request, or null
   * @param clientEvent client's event (for returning version tag)
   * @param returnTombstones whether tombstones should be returned
   * @throws ForceReattemptException
   *           if bucket region is null
   * @return value from the bucket region
   * @throws PrimaryBucketException if the locally managed bucket is not primary
   * @throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed
   */
  public Object getLocally(int bucketId, final Object key,
      final Object aCallbackArgument, boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, boolean returnTombstones) throws PrimaryBucketException,
      ForceReattemptException, PRLocallyDestroyedException
  {
	  return getLocally(bucketId, key,aCallbackArgument, disableCopyOnRead, preferCD, requestingClient, 
			  clientEvent, returnTombstones, false, false);
  }
  /**
   * Returns value corresponding to this key.
   * @param key
   *          the key to look for
   * @param preferCD 
   * @param requestingClient the client making the request, or null
   * @param clientEvent client's event (for returning version tag)
   * @param returnTombstones whether tombstones should be returned
   * @param opScopeIsLocal if true then just check local storage for a value; if false then try to find the value if it is not local
   * @throws ForceReattemptException
   *           if bucket region is null
   * @return value from the bucket region
   * @throws PrimaryBucketException if the locally managed bucket is not primary
   * @throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed
   */
  public Object getLocally(int bucketId, final Object key,
      final Object aCallbackArgument, boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, 
      boolean returnTombstones, boolean opScopeIsLocal, boolean allowReadFromHDFS) throws PrimaryBucketException,
      ForceReattemptException, PRLocallyDestroyedException
  {
    final BucketRegion bucketRegion = getInitializedBucketForId(key, Integer.valueOf(bucketId));
    //  check for primary (when a loader is present) done deeper in the BucketRegion
    Object ret=null;
    if (logger.isDebugEnabled()) {
      logger.debug("getLocally:  key {}) bucketId={}{}{} region {} returnTombstones {} allowReadFromHDFS {}", key,
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, bucketRegion.getName(), returnTombstones, allowReadFromHDFS);
    }
    invokeBucketReadHook();
    try {
      ret = bucketRegion.get(key, aCallbackArgument, true, disableCopyOnRead , preferCD, requestingClient, clientEvent, returnTombstones, opScopeIsLocal, allowReadFromHDFS, false);
      checkIfBucketMoved(bucketRegion);
    }
    catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during get", rde);
        } else {
          throw new InternalGemFireError("Got region destroyed message, but neither bucket nor PR was destroyed", rde);
        }
      }
    }
    return ret; 
  }

  /**
   * Return a value from the bucket region, always serialized
   * @param keyInfo TODO
   * @param clientEvent a "client" event that will hold version information about the entry
   * @param returnTombstones TODO
   * @throws ForceReattemptException
   *           if bucket region is null
   * @return value from the bucket region
   * @throws PrimaryBucketException if the locally managed bucket is not primary
   * @see #getLocally(int, Object, Object, boolean, boolean, ClientProxyMembershipID, EntryEventImpl, boolean)
   */
  public RawValue getSerializedLocally(KeyInfo keyInfo, boolean doNotLockEntry, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS) throws PrimaryBucketException,
      ForceReattemptException {
    final BucketRegion bucketRegion = getInitializedBucketForId(keyInfo.getKey(), keyInfo.getBucketId());
    //  check for primary (when loader is present) done deeper in the BucketRegion
    if (logger.isDebugEnabled()) {
      logger.debug("getSerializedLocally:  key {}) bucketId={}{}{} region {}", keyInfo.getKey(),
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, keyInfo.getBucketId(), bucketRegion.getName());
    }
    invokeBucketReadHook();

    try {
      RawValue result = bucketRegion.getSerialized(keyInfo, true, doNotLockEntry, requestingClient, clientEvent, returnTombstones, allowReadFromHDFS);
      checkIfBucketMoved(bucketRegion);
      return result;
    } catch (RegionDestroyedException rde) {
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during get", rde);
        } else {
          throw rde;
        }
    }
    catch (IOException e) {
      throw new ForceReattemptException(LocalizedStrings.PartitionedRegionDataStore_UNABLE_TO_SERIALIZE_VALUE.toLocalizedString(), e);
    }
  }

  /**
   * Finds the local bucket corresponding to the given key and retrieves the
   * key's Region.Entry
   * @param key
   *          the key to look for
   * @param access
   *          true if caller wants last accessed time updated
   * @param allowTombstones whether a tombstoned entry can be returned
   * 
   * @throws ForceReattemptException
   *           if bucket region is not present in this process
   * @return a RegionEntry for the given key, which will be null if the key is
   *         not in the bucket
   * @throws EntryNotFoundException
   *           TODO-javadocs
   * @throws PRLocallyDestroyedException
   *           if the PartitionRegion is locally destroyed
   */
  public EntrySnapshot getEntryLocally(int bucketId, final Object key,
      boolean access, boolean allowTombstones, boolean allowReadFromHDFS)
      throws EntryNotFoundException, PrimaryBucketException,
      ForceReattemptException, PRLocallyDestroyedException
  {
    final BucketRegion bucketRegion = getInitializedBucketForId(key, Integer.valueOf(bucketId));
    if (logger.isDebugEnabled()) {
      logger.debug("getEntryLocally: key {}) bucketId={}{}{} region {}", key,
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, bucketRegion.getName());
    }
    invokeBucketReadHook();
    EntrySnapshot res = null;
    RegionEntry ent = null;
    try {
      if (allowReadFromHDFS) {
        ent = bucketRegion.entries.getEntry(key);
      }
      else {
        ent = bucketRegion.entries.getOperationalEntryInVM(key);
      }

      if (ent == null) {
        this.getPartitionedRegion().checkReadiness();
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during getEntry");
        }
        throw new EntryNotFoundException(LocalizedStrings.PartitionedRegionDataStore_ENTRY_NOT_FOUND.toLocalizedString());

      // TODO:KIRK:OK } else if ((ent.isTombstone() && allowTombstones) || !Token.isRemoved(ent.getValueInVM(getPartitionedRegion()))) {
      } else if ((ent.isTombstone() && allowTombstones) || !ent.isDestroyedOrRemoved()) {
        res = new EntrySnapshot(ent, bucketRegion,partitionedRegion, allowTombstones);
      }
      checkIfBucketMoved(bucketRegion);
    }
    catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (bucketRegion.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during getEntry", rde);
        } else {
          throw new RegionDestroyedException(LocalizedStrings.PartitionedRegionDataStore_UNABLE_TO_GET_ENTRY.toLocalizedString(), this.partitionedRegion.toString(), rde);
        }
      }
    } finally {
      if (access) {
        bucketRegion.updateStatsForGet(ent, res != null);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("getEntryLocally returning {}", res);
    }
    return res;
  }

  /**
   * Handle a remote request for keys for the provided bucketId
   * 
   * @param bucketId
   * @param interestType
   * @param interestArg
   * @param allowTombstones whether to return destroyed entries
   * @return The <code>Set</code> of keys for bucketId or
   *         {@link Collections#EMPTY_SET}if no keys are present
   *@throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed         
   */
  public Set handleRemoteGetKeys(Integer bucketId, int interestType,
      Object interestArg, boolean allowTombstones) throws PRLocallyDestroyedException, ForceReattemptException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("handleRemoteGetKeys: bucketId: {}{}{} with tombstones {}",
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, allowTombstones);
    }
    Set ret = Collections.EMPTY_SET;
    final BucketRegion r = getInitializedBucketForId(null, bucketId);
    try{
    if (r != null) {
      invokeBucketReadHook();
      if (!r.isEmpty() || (allowTombstones && r.getTombstoneCount() > 0)) {
        ret = r.getKeysWithInterest(interestType, interestArg, allowTombstones);
      }
      checkIfBucketMoved(r);
    }
    }
    catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (r != null && r.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during remoteGetKeys", rde);
        } else {
          throw new RegionDestroyedException(LocalizedStrings.PartitionedRegionDataStore_UNABLE_TO_FETCH_KEYS_ON_0.toLocalizedString(this.partitionedRegion.toString()), this.partitionedRegion.getFullPath(), rde);
        }
      }
    }
    return ret;
  }

  /**
   * Get the local keys for a given bucket.  This operation should be as efficient as possible,
   * by avoiding making copies of the returned set.  The returned set can and should
   * reflect concurrent changes (no ConcurrentModificationExceptions).
   * 
   * @param bucketId
   * @param allowTombstones whether to include destroyed entries in the result
   * @return The <code>Set</code> of keys for bucketId or
   *         {@link Collections#EMPTY_SET} if no keys are present
   *@throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed         
   */
  public Set getKeysLocally(Integer bucketId, boolean allowTombstones) throws PRLocallyDestroyedException, ForceReattemptException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("handleRemoteGetKeys: bucketId: {}{}{}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId);
    }
    Set ret = Collections.EMPTY_SET;
    final BucketRegion r = getInitializedBucketForId(null, bucketId);
    invokeBucketReadHook();
    try{
      if (r != null) {
        Set keys = r.keySet(allowTombstones);
        if (getPartitionedRegion().isHDFSReadWriteRegion()) {
          // hdfs regions can't copy all keys into memory
          ret = keys;

        } else  { 
        // A copy is made so that the bucket is free to move
        ret = new HashSet(r.keySet(allowTombstones));
		}
        checkIfBucketMoved(r);
      }
    }
    catch (RegionDestroyedException rde) {
      if (this.partitionedRegion.isLocallyDestroyed
          || this.partitionedRegion.isClosed) {
        throw new PRLocallyDestroyedException(rde);
      }
      else {
        this.getPartitionedRegion().checkReadiness();
        if (r.isBucketDestroyed()) {
          // bucket moved by rebalance
          throw new ForceReattemptException("Bucket removed during keySet", rde);
        } else {
          throw new RegionDestroyedException(
            LocalizedStrings.PartitionedRegionDataStore_UNABLE_TO_FETCH_KEYS_ON_0
              .toLocalizedString(this.partitionedRegion),
            this.partitionedRegion.getFullPath(), rde);
        }
      }
    }
    return ret;
  }

  
  @Override
  public String toString()
  {
    if (this.partitionedRegion != null) {
      String rName = this.partitionedRegion.getFullPath();
      return this.partitionedRegion.getMyId() + "@" + getClass().getName() + "@"
      + System.identityHashCode(this) + " name: " + rName 
      + " bucket count: " + this.localBucket2RegionMap.size();
    } 
    return null;
  }

  /**
   * Creates the entry with the given key locally. <br>
   * Steps: <br>
   * 1) It finds out the bucket region for the bucket id. <br>
   * 2) If from step 1 it gets null, that means the bucket is remapped. <br>
   * 3) If it finds the bucket region from step 1, it tries to creates the entry
   * on the region. <br>
   * 4) If entry already exists, for the key, step 3 would throw
   * EntryExistsException or else it will create an entry <br>
   * 5) updateBucket2Size if bucket is on more than 1 node or else bucket
   * listners would take care of size update. <br>
   * 
   * @param bucketRegion
   *          the bucket to do the create in
   * @param event
   *          the particulars of the operation
   * @param ifNew
   *          whether a new entry can be created
   * @param ifOld
   *          whether an existing entry can be updated
   * @param lastModified
   *          timestamp
   * @throws ForceReattemptException
   *           if bucket region is null 
   * @throws EntryExistsException
   *           if an entry with this key already exists
   */
  public boolean createLocally(final BucketRegion bucketRegion,
                               final EntryEventImpl event,
                               boolean ifNew,
                               boolean ifOld,
                               boolean requireOldValue,
                               final long lastModified)
  throws ForceReattemptException {
    boolean result = false;
    try{
      event.setRegion(bucketRegion); // convert to the bucket region
      if (event.isOriginRemote()) {
        result = bucketRegion.basicUpdate(event, ifNew, ifOld,
            lastModified, true);
      }
      else {
        // Skip validating again
        result = bucketRegion.virtualPut(event,
                                         ifNew,
                                         ifOld,
                                         null, // expectedOldValue
                                         requireOldValue,
                                         lastModified,
                                         false);
      }
//      if (shouldThrowExists && !posDup) {
//        throw new EntryExistsException(event.getKey().toString());
//      }
      // bug 34361: don't send a reply if bucket was destroyed during the op
      bucketRegion.checkReadiness();
    }
    catch(RegionDestroyedException rde){
      checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(), rde);
    } 
    

    return result;

    // this is now done elsewhere
    //event.setRegion(this.partitionedRegion);
    //this.partitionedRegion.notifyBridgeClients(EnumListenerEvent.AFTER_CREATE,
    //    event);
    //this.partitionedRegion.notifyGatewayHub(EnumListenerEvent.AFTER_CREATE,
    //    event);
  }

  /**
   * Handles the local request to invalidate the key from this region. <br>
   * Steps: <br>
   * 1) Locates the bucket region. If it doesnt find the actual bucket, it means
   * that this bucket is remapped to some other node and remappedBucketException
   * is thrown <br>
   * 2) get the existing value for the key from bucket region <br>
   * 3) if step 2 returns null, throw EntryNotFoundException <br>
   * 4) if step 2 returns non-null value, perform invalidate on the bucket
   * region and use value from step 2 in step 5 <br>
   * 5) updateBucket2Size if bucket is on more than 1 node or else bucket
   * listners would take care of size update. <br>
   * 
   * @param bucketId
   *          the bucketId for the key
   * @param event
   *          the event that prompted this action
   * @throws ForceReattemptException
   *           if bucket region is null 
   * @throws EntryNotFoundException
   *           if entry is not found for the key
   * @throws PrimaryBucketException if the locally managed buffer is not primary
   */
  protected void invalidateLocally(Integer bucketId, EntryEventImpl event)
      throws EntryNotFoundException, PrimaryBucketException,
      ForceReattemptException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("invalidateLocally: bucketId={}{}{} for key={}",
          this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId,  event.getKey());
    }
    final BucketRegion bucketRegion = getInitializedBucketForId(event.getKey(), bucketId);
    try {
      event.setRegion(bucketRegion);
      event.setOldValueFromRegion();
      bucketRegion.basicInvalidate(event);
    
    // bug 34361: don't send a reply if bucket was destroyed during the op
    bucketRegion.checkReadiness();

    // this is now done elsewhere
    //event.setRegion(this.partitionedRegion);
    //this.partitionedRegion.notifyBridgeClients(
    //    EnumListenerEvent.AFTER_INVALIDATE, event);
    } catch(RegionDestroyedException rde){
      checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(), rde);
    } 
  }
  /**
   * This method iterates over localBucket2RegionMap and returns collective size
   * of the bucket regions. <br>
   * Steps: <br>
   * 1) Check if localBucket2RegionMap is empty. If it is, return 0.<br>
   * 2) If localBucket2RegionMap is not empty, get keyset of all these bucket
   * IDs. <br>
   * 3) Get the nodeList for the bucket ID from Bucket2Node region. <br>
   * 4) If first node from the node list is current node, increment the size
   * counter. <br>
   * 5) Step#4 takes care of the problem of recounting the size of redundant
   * buckets. <br>
   * 
   * 
   * @return the map of bucketIds and their associated sizes, or
   *         {@link Collections#EMPTY_MAP}when the size is zero
   */
  public Map getSizeLocally() {
    return getSizeLocally(false);
  }
  
  /**
   * @see #getSizeLocally()
   * @param primaryOnly true if sizes for primary buckets are desired
   * @return the map of bucketIds and their associated sizes
   */
  public Map<Integer, Integer> getSizeLocally(boolean primaryOnly)
  {
    HashMap<Integer, Integer> mySizeMap;
    if (this.localBucket2RegionMap.isEmpty()) {
      return Collections.EMPTY_MAP;
    }
    mySizeMap = new HashMap<Integer, Integer>(this.localBucket2RegionMap.size());
    Map.Entry<Integer, BucketRegion> me;
    BucketRegion r;
    for (Iterator<Map.Entry<Integer, BucketRegion>> itr = this.localBucket2RegionMap.entrySet().iterator(); itr.hasNext(); ) {
      me = itr.next();
      try {
        r = me.getValue();
        if(null != r) { // fix for bug#35497
          r.waitForData();
          if (primaryOnly) {
            if (r.getBucketAdvisor().isPrimary()) {
              mySizeMap.put(me.getKey(), r.size());
            }
          } else {
            mySizeMap.put(me.getKey(), r.size());
          }
        }
      } catch (CacheRuntimeException skip) {
        continue;
      }
    } // for
    if (logger.isDebugEnabled()) {
      logger.debug("getSizeLocally: returns bucketSizes={}", mySizeMap);
    }

    return mySizeMap;
  }
  
  /**
   * This method iterates over localBucket2RegionMap and returns collective size
   * of the primary bucket regions.  
   * 
   * @return the map of bucketIds and their associated sizes, or
   *         {@link Collections#EMPTY_MAP}when the size is zero
   */
  public Map<Integer, SizeEntry> getSizeForLocalBuckets()
  {
    return getSizeLocallyForBuckets(this.localBucket2RegionMap.keySet());
  }
  
  public Map<Integer, SizeEntry> getSizeForLocalPrimaryBuckets() {
    return getSizeLocallyForBuckets(getAllLocalPrimaryBucketIds());
  }

  public Map<Integer, SizeEntry> getSizeEstimateForLocalPrimaryBuckets() {
    return getSizeEstimateLocallyForBuckets(getAllLocalPrimaryBucketIds());
  }

  /**
   * This calculates size of all the primary bucket regions for the list of bucketIds.
   * 
   * @param bucketIds
   * @return the size of all the primary bucket regions for the list of bucketIds.
   */
  public Map<Integer, SizeEntry> getSizeLocallyForBuckets(Collection<Integer> bucketIds) {
    return getSizeLocallyForPrimary(bucketIds, false);
  }

  public Map<Integer, SizeEntry> getSizeEstimateLocallyForBuckets(Collection<Integer> bucketIds) {
    return getSizeLocallyForPrimary(bucketIds, true);
  }

  private Map<Integer, SizeEntry> getSizeLocallyForPrimary(Collection<Integer> bucketIds, boolean estimate) {
    Map<Integer, SizeEntry> mySizeMap;
    if (this.localBucket2RegionMap.isEmpty()) {
      return Collections.emptyMap();
    }
    mySizeMap = new HashMap<Integer, SizeEntry>(this.localBucket2RegionMap.size());
    BucketRegion r = null;
    for(Integer bucketId : bucketIds) {
      try {
        r = getInitializedBucketForId(null, bucketId);
        mySizeMap.put(bucketId, new SizeEntry(estimate ? r.sizeEstimate() : r.size(), r.getBucketAdvisor().isPrimary()));
//        if (getLogWriter().fineEnabled() && r.getBucketAdvisor().isPrimary()) {
//          r.verifyTombstoneCount();
//        }
      } catch (PrimaryBucketException skip) {
        // sizeEstimate() will throw this exception as it will not try to read from HDFS on a secondary bucket,
        // this bucket will be retried in PartitionedRegion.getSizeForHDFS() fixes bug 49033
        continue;
      } catch (ForceReattemptException skip) {
        continue;
      } catch(RegionDestroyedException skip) {
        continue;
      }
    } // while
    return mySizeMap;
  }

  public int getSizeOfLocalPrimaryBuckets() {
    int sizeOfLocalPrimaries = 0;
    Set<BucketRegion> primaryBuckets = getAllLocalPrimaryBucketRegions();
    for (BucketRegion br : primaryBuckets) {
      sizeOfLocalPrimaries += br.size();
    }
    return sizeOfLocalPrimaries;
  }

    
  /**
   * Interface for visiting buckets 
   * @author Mitch Thomas
   */
  // public visibility for tests
  public static abstract class BucketVisitor {
    abstract public void visit(Integer bucketId, Region r);
  }
  // public visibility for tests
  public void visitBuckets(final BucketVisitor bv) {
    if (this.localBucket2RegionMap.size() > 0) {
      Map.Entry me;
      for (Iterator i = this.localBucket2RegionMap.entrySet().iterator(); i.hasNext(); ) {
        me = (Map.Entry) i.next();
        Region r = (Region) me.getValue();
        // ConcurrentHashMap entrySet iterator does not guarantee an atomic snapshot
        // of an entry.  Specifically, getValue() performs a CHM.get() and as a result
        // may return null if the entry was removed, but yet always returns a key 
        // under the same circumstances... Ouch.  Due to the fact that entries are 
        // removed as part of data store performs cleanup, a null check is required
        // to protect BucketVisitors, in the event iteration occurs during data 
        // store cleanup.  Bug fix 38680.
        if (r != null) {
          bv.visit((Integer) me.getKey(), r);
        }
      }
    } 
  }
  private void visitBucket(final Integer bucketId, final LocalRegion bucket, final EntryVisitor ev) {
    try {
      for (Iterator ei = bucket.entrySet().iterator(); ei.hasNext(); ) {
        ev.visit(bucketId, (Region.Entry) ei.next());
      }
    } catch (CacheRuntimeException ignore) {}
    ev.finishedVisiting();
  }
  
  /**
   * Test class and method for visiting Entries
   * NOTE: This class will only give a partial view if a visited bucket is moved
   * by a rebalance while a visit is in progress on that bucket.
   * @author mthomas
   */
  protected static abstract class EntryVisitor  {
    abstract public void visit(Integer bucketId, Region.Entry re);
    abstract public void finishedVisiting();
  }
  private void visitEntries(final EntryVisitor knock) {
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region buk)
      {
        try {
          ((LocalRegion)buk).waitForData();
          for (Iterator ei = buk.entrySet().iterator(); ei.hasNext(); ) {
            knock.visit(bucketId, (Region.Entry) ei.next());
          }
        } catch (CacheRuntimeException ignore) {}
       knock.finishedVisiting();
      }
    });
  } 
  
  /**
   * <i>Test Method</i>
   * Return the list of PartitionedRegion entries contained in this data store  
   * @return a List of all entries gathered across all buckets in this data store
   */
  public final List getEntries() {
    final ArrayList al = new ArrayList();
    visitEntries(new EntryVisitor() {
      @Override
      public void visit(Integer bucketId, Entry re)
      {
        if (re.getValue() != Token.TOMBSTONE) {
          al.add(re);
        }
      }
      @Override
      public void finishedVisiting() {}
    });
    return al;
  }
  
  /**
   * <i>Test Method</i>
   * Dump all the entries in all the buckets to the logger, validate that the 
   * bucket-to-node meta region contains all bhe buckets managed by this data store
   * @param validateOnly only perform bucket-to-node validation
   */
  public final void dumpEntries(final boolean validateOnly) {
    if (logger.isDebugEnabled()) {
      logger.debug("[dumpEntries] dumping {}", this);
    }
    final StringBuffer buf;
    if (validateOnly) {
      buf = null;
      
      // If we're doing a validation, make sure the region is initialized
      // before insisting that its metadata be correct :-)
      this.partitionedRegion.waitForData();
    } else {
      dumpBackingMaps();
    }
  }
  
  public final void dumpBackingMaps() {
    if (logger.isDebugEnabled()) {
      logger.debug("Bucket maps in {}\n", this);
    }
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region buk)
      {
        try {
          LocalRegion lbuk = (LocalRegion)buk;
          lbuk.waitForData();
          int size = lbuk.size();
          int keySetSize = (new HashSet(lbuk.keySet())).size();
          if (size != keySetSize) {
            if (logger.isDebugEnabled()) {
              logger.debug("Size is not consistent with keySet size! size={} but keySet size={} region={}", size, keySetSize, lbuk);
            }
          }
          lbuk.dumpBackingMap();
        } catch (CacheRuntimeException ignore) {}
      }
    });
  }

  
  /**
   * <i>Test Method</i>
   * Dump all the bucket names in this data store to the logger 
   * 
   */
  public final void dumpBuckets() {
    final StringBuffer buf = new StringBuffer("Buckets in ").append(this).append("\n");
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region r)
      {
        buf.append("bucketId: ").append(partitionedRegion.bucketStringForLogs(bucketId.intValue())).append(" bucketName: ").append(r).append("\n");
      }
    });
    logger.debug(buf.toString());
  }
  
  /**
   * <i>Test Method</i> Return the list of all the bucket names in this data
   * store.
   * 
   */
  public final List getLocalBucketsListTestOnly() {
    final List bucketList = new ArrayList();
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region r) {
        bucketList.add(bucketId);
      }
    });
    return bucketList;
  }
  
  /**
   * <i>Test Method</i> Return the list of all the primary bucket ids in this
   * data store.
   * 
   */
  public final List getLocalPrimaryBucketsListTestOnly() {
    final List primaryBucketList = new ArrayList();
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region r) {
        BucketRegion br = (BucketRegion)r;
        BucketAdvisor ba = (BucketAdvisor)br.getDistributionAdvisor();
        if (ba.isPrimary()) {
          primaryBucketList.add(bucketId);
        }
      }
    });
    return primaryBucketList;
  }
  
  /**
   * <i>Test Method</i> Return the list of all the non primary bucket ids in this
   * data store.
   * 
   */
  public final List getLocalNonPrimaryBucketsListTestOnly() {
    final List nonPrimaryBucketList = new ArrayList();
    visitBuckets(new BucketVisitor() {
      @Override
      public void visit(Integer bucketId, Region r) {
        BucketRegion br = (BucketRegion)r;
        BucketAdvisor ba = (BucketAdvisor)br.getDistributionAdvisor();
        if (!ba.isPrimary()) {
          nonPrimaryBucketList.add(bucketId);
        }
      }
    });
    return nonPrimaryBucketList;
  }
  
  /**
   * <i>Test Method</i>
   * Dump the entries in this given bucket to the logger
   * @param bucketId the id of the bucket to dump
   * @param bucket the Region containing the bucket data
   */
  public final void dumpBucket(int bucketId, final LocalRegion bucket) {
    Integer buckId = Integer.valueOf(bucketId);
    visitBucket(buckId, bucket, new EntryVisitor() {
      final StringBuffer buf = new StringBuffer("Entries in bucket ").append(bucket).append("\n");
      @Override
      public void visit(Integer bid, Entry re)
      {
        buf.append(re.getKey()).append(" => ").append(re.getValue()).append("\n"); 
      }

      @Override
      public void finishedVisiting() {
        logger.debug(buf.toString());
      }
    });

  } 
  

  /**
   * Fetch the entries for the given bucket 
   * @param bucketId the id of the bucket
   * @return  a Map containing all the entries
   */
  public BucketRegion handleRemoteGetEntries(int bucketId)
      throws ForceReattemptException  {
    if (logger.isDebugEnabled()) {
      logger.debug("handleRemoteGetEntries: bucketId: {}{}{}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId); 
    }
    BucketRegion br = getInitializedBucketForId(null, Integer.valueOf(bucketId));
    // NOTE: this is a test method that does not take a snapshot so it does not
    // give a stable set of entries if the bucket is moved during a rebalance
    return br;
  }

  public CachePerfStats getCachePerfStats()
  {
    return this.bucketStats;
  }
  
  /**
   * Return a set of local buckets.  Iterators may include entries with null 
   * values (but non-null keys).  
   * 
   * @return an unmodifiable set of Map.Entry objects
   */
  public Set<Map.Entry<Integer, BucketRegion>> getAllLocalBuckets() {
    return Collections.unmodifiableSet(localBucket2RegionMap.entrySet());
  }
  
  public Set<Integer> getAllLocalBucketIds() {
    return Collections.unmodifiableSet(localBucket2RegionMap.keySet());
  }

  /**
   * Returns a set of local buckets. 
   * @return an unmodifiable set of BucketRegion
   */
  public Set<BucketRegion> getAllLocalBucketRegions() {
    Set<BucketRegion> retVal = new HashSet<BucketRegion>();
    for (BucketRegion br : localBucket2RegionMap.values()) {
      retVal.add(br);
    }
    return Collections.unmodifiableSet(retVal);
  }
  
  public boolean isLocalBucketRegionPresent(){
    return localBucket2RegionMap.size() > 0;
  }

  public Set<BucketRegion> getAllLocalPrimaryBucketRegions() {
    Set<BucketRegion> retVal = new HashSet<BucketRegion>();
    for (BucketRegion br : localBucket2RegionMap.values()) {
      if (br.getBucketAdvisor().isPrimary()) {
        retVal.add(br);
      }
    }
    return Collections.unmodifiableSet(retVal);
  }

  public Set<Integer> getAllLocalPrimaryBucketIds() {
    Set<Integer> bucketIds = new HashSet<Integer>();
    for (Map.Entry<Integer, BucketRegion> bucketEntry : getAllLocalBuckets()) {
      BucketRegion bucket = bucketEntry.getValue();
      if (bucket.getBucketAdvisor().isPrimary()) {
        bucketIds.add(Integer.valueOf(bucket.getId()));
      }
    }
    return bucketIds;
  }
  
  public Set<Integer> getAllLocalPrimaryBucketIdsBetweenProvidedIds(int low,
      int high) {
    Set<Integer> bucketIds = new HashSet<Integer>();
    for (Map.Entry<Integer, BucketRegion> bucketEntry : getAllLocalBuckets()) {
      BucketRegion bucket = bucketEntry.getValue();
      if (bucket.getBucketAdvisor().isPrimary() && (bucket.getId() >= low)
          && (bucket.getId() < high)) {
        bucketIds.add(Integer.valueOf(bucket.getId()));
      }
    }
    return bucketIds;
  }

  /** a fast estimate of total bucket size */
  public long getEstimatedLocalBucketSize(boolean primaryOnly) {
    long size = 0;
    for (BucketRegion br : localBucket2RegionMap.values()) {
      if (!primaryOnly || br.getBucketAdvisor().isPrimary()) {
        size += br.getEstimatedLocalSize();
      }
    }
    return size;
  }

  /** a fast estimate of total bucket size */
  public long getEstimatedLocalBucketSize(Set<Integer> bucketIds) {
    long size = 0;
    for (Integer bid : bucketIds) {
      BucketRegion br = localBucket2RegionMap.get(bid);
      if (br != null) {
        size += br.getEstimatedLocalSize();
      }
    }
    return size;
  }

  public Object getLocalValueInVM(final Object key, int bucketId)
  {
    try {
      BucketRegion br = getInitializedBucketForId(key, Integer.valueOf(bucketId));
      return br.getValueInVM(key);
    }
    catch (ForceReattemptException e) {
      e.checkKey(key);
      return null;
    }
  }
  
  /**
   * This method is intended for testing purposes only.
   * DO NOT use in product code.
   */
  public Object getLocalValueOnDisk(final Object key, int bucketId)
  {
    try {
      BucketRegion br = getInitializedBucketForId(key, Integer.valueOf(bucketId));
      return br.getValueOnDisk(key);
    }
    catch (ForceReattemptException e) {
      e.checkKey(key);
      return null;
    }
  }
  
  public Object getLocalValueOnDiskOrBuffer(final Object key, int bucketId)
  {
    try {
      BucketRegion br = getInitializedBucketForId(key, Integer.valueOf(bucketId));
      return br.getValueOnDiskOrBuffer(key);
    }
    catch (ForceReattemptException e) {
      e.checkKey(key);
      return null;
    }
  }
  
  /**
   * Checks for RegionDestroyedException 
   * in case of remoteEvent & localDestroy OR isClosed
   * throws a ForceReattemptException
   * @param br the bucket that we are trying to operate on
   * @param  isOriginRemote true the event we are processing has a remote origin.
   * @param  rde
   *  
   */
  public void checkRegionDestroyedOnBucket(final BucketRegion br,
                                            final boolean isOriginRemote,
                                            RegionDestroyedException rde)
  throws ForceReattemptException {
    if (isOriginRemote){
      if (logger.isDebugEnabled()) {
        logger.debug("Operation failed due to RegionDestroyedException", rde);
      }
      if (this.partitionedRegion.isLocallyDestroyed || this.partitionedRegion.isClosed) {
        throw new ForceReattemptException("Operation failed due to RegionDestroyedException :"+rde , rde);
      } else {
        this.partitionedRegion.checkReadiness();
        if (br.isBucketDestroyed()) {
          throw new ForceReattemptException("Bucket moved", rde);
        }
      }
    } else {
      // this can now happen due to a rebalance removing a bucket
      this.partitionedRegion.checkReadiness();
      if (br.isBucketDestroyed()) {
        throw new ForceReattemptException("Bucket moved", rde);
      }
    }
    throw new InternalGemFireError("Got region destroyed message, but neither bucket nor PR was destroyed", rde);
  }

  /**
   * Create a redundancy bucket on this member
   * 
   * @param bucketId
   *          the id of the bucket to create
   * @param moveSource
   *          the member id of where the bucket is being copied from, if this is
   *          a bucket move. Setting this field means that we are allowed to go
   *          over redundancy.
   * @param forceCreation
   *          force the bucket creation, even if it would provide better balance
   *          if the bucket was placed on another node.
   * @param replaceOffineData
   *          create the bucket, even if redundancy is satisfied when
   *          considering offline members.
   * @param isRebalance
   *          true if this is a rebalance
   * @param creationRequestor
   *          the id of the member that is atomically creating this bucket on
   *          all members, if this is an atomic bucket creation.
   * @return the status of the bucket creation.
   */
  public CreateBucketResult grabBucket(final int bucketId, 
                            final InternalDistributedMember moveSource,
                            final boolean forceCreation,
                            final boolean replaceOffineData,
                            final boolean isRebalance,
                            final InternalDistributedMember creationRequestor,
                            final boolean isDiskRecovery) {
    CreateBucketResult grab = grabFreeBucket(bucketId, partitionedRegion.getMyId(), 
        moveSource, forceCreation, isRebalance, true, replaceOffineData, creationRequestor);
    if (!grab.nowExists()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed grab for bucketId = {}{}{}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId);
      }
      //   Assert.assertTrue(nList.contains(partitionedRegion.getNode().getMemberId()) , 
      // " grab returned false and b2n does not contains this member.");
    } else {
      // try grabbing bucekts for all the PR which are colocated with it
      List colocatedWithList = ColocationHelper.getColocatedChildRegions(partitionedRegion);
      Iterator itr = colocatedWithList.iterator();
      while (itr.hasNext()) {
        PartitionedRegion pr = (PartitionedRegion)itr.next();
        if (logger.isDebugEnabled()) {
          logger.debug("For bucketId = {} isInitialized {} iscolocation complete {} pr name {}",
              bucketId, pr.isInitialized(), pr.getDataStore().isColocationComplete(bucketId), pr.getFullPath());
        }
        if ((isDiskRecovery || pr.isInitialized())
            && (pr.getDataStore().isColocationComplete(
                bucketId))) {
          grab = pr.getDataStore().grabFreeBucketRecursively(
              bucketId, pr, moveSource, forceCreation, replaceOffineData, isRebalance, creationRequestor, isDiskRecovery);
          if (!grab.nowExists()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Failed grab for bucketId = {}{}{}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId);
            }
            // Should Throw Exception-- As discussed in weekly call
            // " grab returned false and b2n does not contains this member.");
          }
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Grab attempt on bucketId={}{}{}; grab:{}",this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bucketId, grab); 
    }
    return grab;
  }

  /**
   * Checks consistency of bucket and meta data before attempting to grab the
   * bucket.
   * 
   * @param buckId
   * @return false if bucket should not be grabbed, else true.
   * TODO prpersist - move this to BucketRegion
   */
  public boolean verifyBucketBeforeGrabbing(final int buckId) {
    // Consistency checks
    final boolean isNodeInMetaData = partitionedRegion.getRegionAdvisor()
        .isBucketLocal(buckId);
    if (isManagingBucket(buckId)) {
      if (!isNodeInMetaData) {
        partitionedRegion.checkReadiness();
        Set owners = partitionedRegion.getRegionAdvisor().getBucketOwners(
            buckId);
        logger.info(LocalizedMessage.create(
                LocalizedStrings.PartitionedRegionDataStore_VERIFIED_NODELIST_FOR_BUCKETID_0_IS_1,
                new Object[] { partitionedRegion.bucketStringForLogs(buckId),
                    PartitionedRegionHelper.printCollection(owners) }));
        Assert.assertTrue(false, " This node " + partitionedRegion.getNode()
            + " is managing the bucket with bucketId= "
            + partitionedRegion.bucketStringForLogs(buckId)
            + " but doesn't have an entry in " + "b2n region for PR "
            + partitionedRegion);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("BR#verifyBucketBeforeGrabbing We already host {}{}{}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, buckId);
      }
      //It's ok to return true here, we do another check later
      //to make sure we don't host the bucket.
      return true;
    }
    else {
      if (partitionedRegion.isDestroyed()
          || partitionedRegion.getGemFireCache().isClosed()) {
        if (logger.isDebugEnabled()) {
          logger.debug("BR#verifyBucketBeforeGrabbing: Exiting early due to Region destruction");
        }
        return false;
      }
      if (isNodeInMetaData) {
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegionDataStore: grabBackupBuckets: This node is not managing the bucket with Id = {} but has an entry in the b2n region for PartitionedRegion {} because destruction of this PartitionedRegion is initiated on other node",
              buckId, partitionedRegion);
        }
      }
    } // End consistency checks
    return true;
  }

  public void executeOnDataStore(final Set localKeys, final Function function,
      final Object object, final int prid, final Set<Integer> bucketSet,
      final boolean isReExecute,
      final PartitionedRegionFunctionStreamingMessage msg, long time,
      ServerConnection servConn, int transactionID){
    
    if (!areAllBucketsHosted(bucketSet)) {
      throw new BucketMovedException(
          LocalizedStrings.FunctionService_BUCKET_MIGRATED_TO_ANOTHER_NODE
              .toLocalizedString());
    }
    final DM dm = this.partitionedRegion.getDistributionManager();
    
    ResultSender resultSender = new PartitionedRegionFunctionResultSender(dm,
        this.partitionedRegion, time, msg, function, bucketSet);  

    final RegionFunctionContextImpl prContext = new RegionFunctionContextImpl(
        function.getId(), this.partitionedRegion, object, localKeys,
        ColocationHelper.constructAndGetAllColocatedLocalDataSet(
            this.partitionedRegion, bucketSet), bucketSet, resultSender,
        isReExecute);

    FunctionStats stats = FunctionStats.getFunctionStats(function.getId(), dm.getSystem());
    try {
      long start = stats.startTime();
      stats.startFunctionExecution(function.hasResult());
      if (logger.isDebugEnabled()) {
        logger.debug("Executing Function: {} on Remote Node with context: ", function.getId(), prContext);
      }
      function.execute(prContext);
      stats.endFunctionExecution(start, function.hasResult());
    }
    catch (FunctionException functionException) {
      if (logger.isDebugEnabled()) {
        logger.debug("FunctionException occured on remote node while executing Function: {}", function.getId(), functionException);
      }
      stats.endFunctionExecutionWithException(function.hasResult());
      if(functionException.getCause() instanceof QueryInvalidException) {
        // Handle this exception differently since it can contain
        // non-serializable objects.
        // java.io.NotSerializableException: antlr.CommonToken
        // create a new FunctionException on the original one's message (not cause).
        throw new FunctionException(functionException.getLocalizedMessage());
      } 
      throw functionException ;
    }
  }
  
  public boolean areAllBucketsHosted(final Set<Integer> bucketSet) {
//    boolean arr[] = new boolean[]{false, true, false, true , false , false , false , false };
//    Random random = new Random();
//    int num = random.nextInt(7);
//    System.out.println("PRDS.verifyBuckets returning " + arr[num]);
//    return arr[num];
    for (Integer bucket : bucketSet) {
      if (!this.partitionedRegion.getRegionAdvisor().getBucketAdvisor(bucket)
          .isHosting()) {
        return false;
      }
    }
    return true;
  }
  
  
  /*
   * @return true if there is a local bucket for the event and it has seen the event
   */
  public boolean hasSeenEvent(EntryEventImpl event) {
    BucketRegion bucket = getLocalBucketById(event.getKeyInfo().getBucketId());
    if (bucket == null) {
      return false;
    } else {
      return bucket.hasSeenEvent(event);
    }
  }
  
  public void handleInterestEvent(InterestRegistrationEvent event) {
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegionDataStore for {} handling {}", this.partitionedRegion.getFullPath(), event);
    }
    synchronized (keysOfInterestLock) {
      boolean isRegister = event.isRegister();
      for (Iterator i = event.getKeysOfInterest().iterator(); i.hasNext();) {
        Object key = i.next();
        // Get the reference counter for this key
        AtomicInteger references = (AtomicInteger)this.keysOfInterest.get(key);
        int newNumberOfReferences = 0;
        // If this is a registration event, add interest for this key
        if (isRegister) {
          if (logger.isDebugEnabled()) {
            logger.debug("PartitionedRegionDataStore for {} adding interest for: ", this.partitionedRegion.getFullPath(), key);
          }
          if (references == null) {
            references = new AtomicInteger();
            this.keysOfInterest.put(key, references);
          }
          newNumberOfReferences = references.incrementAndGet();
        }
        else {
          // If this is an unregistration event, remove interest for this key
          if (logger.isDebugEnabled()) {
            logger.debug("PartitionedRegionDataStore for {} removing interest for: ", this.partitionedRegion.getFullPath(), key);
          }
          if (references != null) {
            newNumberOfReferences = references.decrementAndGet();
            // If there are no more references, remove this key
            if (newNumberOfReferences == 0) {
              this.keysOfInterest.remove(key);
            }
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegionDataStore for {} now has {} client(s) interested in key {}",
              this.partitionedRegion.getFullPath(), newNumberOfReferences, key);
        }
      }
    }
  }
  
  class BucketAttributesFactory extends AttributesFactory {
    @Override
    protected void setBucketRegion(boolean b){
      super.setBucketRegion(b);
    }    
  }
  
  public enum CreateBucketResult {
    /** Indicates that the bucket was successfully created on this node */
    CREATED(true),
    /** Indicates that the bucket creation failed */
    FAILED(false),
    /** Indicates that the bucket already exists on this node */
    ALREADY_EXISTS(true),
    /** Indicates that redundancy was already satisfied */
    REDUNDANCY_ALREADY_SATISFIED(false);
    
    private final boolean nowExists;
    
    private CreateBucketResult(boolean nowExists) {
      this.nowExists = nowExists;
    }
    
    boolean nowExists() {
      return this.nowExists;
    }
  }

  public void updateEntryVersionLocally(Integer bucketId, EntryEventImpl event) throws ForceReattemptException {

    if (logger.isDebugEnabled()) {
      logger.debug("updateEntryVersionLocally: bucketId={}{}{} for key={}", this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR,
          bucketId, event.getKey());
    }
    final BucketRegion bucketRegion = getInitializedBucketForId(event.getKey(), bucketId);
    try {
      event.setRegion(bucketRegion);
      bucketRegion.basicUpdateEntryVersion(event);
    
      // bug 34361: don't send a reply if bucket was destroyed during the op
      bucketRegion.checkReadiness();

    } catch(RegionDestroyedException rde){
      checkRegionDestroyedOnBucket(bucketRegion, event.isOriginRemote(), rde);
    } 
  }
}
