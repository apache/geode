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

package org.apache.geode.internal.cache;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.DeltaSerializationException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.AtomicLongWithTerminalState;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.CreateRegionProcessor.CreateRegionReplyProcessor;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.event.EventSequenceNumberHolder;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.DestroyMessage;
import org.apache.geode.internal.cache.partitioned.InvalidateMessage;
import org.apache.geode.internal.cache.partitioned.LockObject;
import org.apache.geode.internal.cache.partitioned.PRTombstoneMessage;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage;
import org.apache.geode.internal.cache.partitioned.PutMessage;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientTombstoneMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.concurrent.AtomicLong5;
import org.apache.geode.internal.concurrent.Atomics;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * The storage used for a Partitioned Region. This class asserts distributed scope as well as a
 * replicate data policy It does not support transactions
 *
 * Primary election for a BucketRegion can be found in the
 * {@link org.apache.geode.internal.cache.BucketAdvisor} class
 *
 * @since GemFire 5.1
 *
 */
public class BucketRegion extends DistributedRegion implements Bucket {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final RawValue NULLVALUE = new RawValue(null);
  @Immutable
  public static final RawValue REQUIRES_ENTRY_LOCK = new RawValue(null);
  /**
   * A special value for the bucket size indicating that this bucket has been destroyed.
   */
  private static final long BUCKET_DESTROYED = Long.MIN_VALUE;
  private AtomicLong counter = new AtomicLong();
  private AtomicLong limit;
  private final AtomicLong numOverflowOnDisk = new AtomicLong();
  private final AtomicLong numOverflowBytesOnDisk = new AtomicLong();
  private final AtomicLong numEntriesInVM = new AtomicLong();
  private final AtomicLong evictions = new AtomicLong();
  // For GII
  private CreateRegionReplyProcessor createRegionReplyProcessor;

  /**
   * Contains size in bytes of the values stored in theRealMap. Sizes are tallied during put and
   * remove operations.
   */
  private final AtomicLongWithTerminalState bytesInMemory = new AtomicLongWithTerminalState();

  public static class RawValue {
    private final Object rawValue;

    public RawValue(Object rawVal) {
      rawValue = rawVal;
    }

    public boolean isValueByteArray() {
      return rawValue instanceof byte[];
    }

    public Object getRawValue() {
      return rawValue;
    }

    public void writeAsByteArray(DataOutput out) throws IOException {
      if (isValueByteArray()) {
        DataSerializer.writeByteArray((byte[]) rawValue, out);
      } else if (rawValue instanceof CachedDeserializable) {
        ((CachedDeserializable) rawValue).writeValueAsByteArray(out);
      } else if (Token.isInvalid(rawValue)) {
        DataSerializer.writeByteArray(null, out);
      } else if (rawValue == Token.TOMBSTONE) {
        DataSerializer.writeByteArray(null, out);
      } else {
        DataSerializer.writeObjectAsByteArray(rawValue, out);
      }
    }

    @Override
    public String toString() {
      return "RawValue(" + rawValue + ")";
    }

    /**
     * Return the de-serialized value without changing the stored form in the heap. This causes
     * local access to create a de-serialized copy (extra work) in favor of keeping values in
     * serialized form which is important because it makes remote access more efficient. This
     * assumption is that remote access is much more frequent. TODO Unused, but keeping for
     * potential performance boost when local Bucket access de-serializes the entry (which could
     * hurt perf.)
     *
     * @return the de-serialized value
     */
    public Object getDeserialized(boolean copyOnRead) {
      if (isValueByteArray()) {
        if (copyOnRead) {
          // TODO move this code to CopyHelper.copy?
          byte[] src = (byte[]) rawValue;
          byte[] dest = new byte[src.length];
          System.arraycopy(rawValue, 0, dest, 0, dest.length);
          return dest;
        } else {
          return rawValue;
        }
      } else if (rawValue instanceof CachedDeserializable) {
        if (copyOnRead) {
          return ((CachedDeserializable) rawValue).getDeserializedWritableCopy(null, null);
        } else {
          return ((CachedDeserializable) rawValue).getDeserializedForReading();
        }
      } else if (Token.isInvalid(rawValue)) {
        return null;
      } else {
        if (copyOnRead) {
          return CopyHelper.copy(rawValue);
        } else {
          return rawValue;
        }
      }
    }
  }

  private final int redundancy;

  /** the partitioned region to which this bucket belongs */
  private final PartitionedRegion partitionedRegion;
  private final Map<Object, ExpiryTask> pendingSecondaryExpires = new HashMap<>();

  /* one map per bucket region */
  private final HashMap<Object, LockObject> allKeysMap = new HashMap<>();

  static final boolean FORCE_LOCAL_LISTENERS_INVOCATION = Boolean
      .getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "BucketRegion.alwaysFireLocalListeners");

  private volatile AtomicLong5 eventSeqNum = null;

  AtomicLong5 getEventSeqNum() {
    return eventSeqNum;
  }

  public BucketRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs,
      StatisticsClock statisticsClock) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs, statisticsClock);
    if (PartitionedRegion.DISABLE_SECONDARY_BUCKET_ACK) {
      Assert.assertTrue(attrs.getScope().isDistributedNoAck());
    } else {
      Assert.assertTrue(attrs.getScope().isDistributedAck());
    }
    Assert.assertTrue(attrs.getDataPolicy().withReplication());
    Assert.assertTrue(!attrs.getEarlyAck());
    Assert.assertTrue(isUsedForPartitionedRegionBucket());
    Assert.assertTrue(!isUsedForPartitionedRegionAdmin());
    Assert.assertTrue(internalRegionArgs.getBucketAdvisor() != null);
    Assert.assertTrue(internalRegionArgs.getPartitionedRegion() != null);
    redundancy = internalRegionArgs.getPartitionedRegionBucketRedundancy();
    partitionedRegion = internalRegionArgs.getPartitionedRegion();
    setEventSeqNum();
  }

  // Attempt to direct the GII process to the primary first
  @Override
  public void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException {
    // Set this region in the ProxyBucketRegion early so that profile exchange will
    // perform the correct fillInProfile method
    getBucketAdvisor().getProxyBucketRegion().setBucketRegion(this);
    boolean success = false;
    try {
      final InternalDistributedMember primaryHolder = getBucketAdvisor().basicGetPrimaryMember();
      if (primaryHolder != null && !primaryHolder.equals(getMyId())) {
        // Ignore the provided image target, use an existing primary (if any)
        super.initialize(snapshotInputStream, primaryHolder, internalRegionArgs);
      } else {
        super.initialize(snapshotInputStream, imageTarget, internalRegionArgs);
      }

      success = true;
    } finally {
      if (!success) {
        removeFromPeersAdvisors(false);
        getBucketAdvisor().getProxyBucketRegion().clearBucketRegion(this);
      }
    }
  }

  private void setEventSeqNum() {
    if (partitionedRegion.isShadowPR() && partitionedRegion.getColocatedWith() != null) {
      PartitionedRegion parentPR = ColocationHelper.getLeaderRegion(partitionedRegion);
      BucketRegion parentBucket = parentPR.getDataStore().getLocalBucketById(getId());
      // needs to be set only once.
      if (parentBucket.eventSeqNum == null) {
        parentBucket.eventSeqNum = new AtomicLong5(getId());
      }
    }
    if (partitionedRegion.getColocatedWith() == null) {
      eventSeqNum = new AtomicLong5(getId());
    } else {
      PartitionedRegion parentPR = ColocationHelper.getLeaderRegion(partitionedRegion);
      BucketRegion parentBucket = parentPR.getDataStore().getLocalBucketById(getId());
      if (parentBucket == null && logger.isDebugEnabled()) {
        logger.debug("The parentBucket of region {} bucketId {} is NULL",
            partitionedRegion.getFullPath(), getId());
      }
      Assert.assertTrue(parentBucket != null);
      eventSeqNum = parentBucket.eventSeqNum;
    }
  }


  @Override
  void initialized() {
    // announce that the bucket is ready
    // setHosting performs a profile exchange, so there
    // is no need to call super.initialized() here.
  }

  @Override
  DiskStoreImpl findDiskStore(RegionAttributes regionAttributes,
      InternalRegionArguments internalRegionArgs) {
    return internalRegionArgs.getPartitionedRegion().getDiskStore();
  }

  @Override
  public void registerCreateRegionReplyProcessor(CreateRegionReplyProcessor processor) {
    createRegionReplyProcessor = processor;
  }

  @Override
  protected void recordEventStateFromImageProvider(InternalDistributedMember provider) {
    if (createRegionReplyProcessor != null) {
      Map<ThreadIdentifier, EventSequenceNumberHolder> providerEventStates =
          createRegionReplyProcessor.getEventState(provider);
      if (providerEventStates != null) {
        recordEventState(provider, providerEventStates);
      } else {
        // Does not see this to happen. Just in case we get gii from a node
        // that was not in the cluster originally when we sent
        // createRegionMessage (its event tracker was saved),
        // but later available before we could get gii from anyone else.
        // This will not cause data inconsistent issue. Log this message for debug purpose.
        logger.info("Could not initiate event tracker from GII provider {}", provider);
      }
      createRegionReplyProcessor = null;
    }
  }

  @Override
  protected CacheDistributionAdvisor createDistributionAdvisor(
      InternalRegionArguments internalRegionArgs) {
    return internalRegionArgs.getBucketAdvisor();
  }

  @Override
  public BucketAdvisor getBucketAdvisor() {
    return (BucketAdvisor) getDistributionAdvisor();
  }

  @Override
  public boolean isHosting() {
    return getBucketAdvisor().isHosting();
  }

  @Override
  protected EventID distributeTombstoneGC(Set<Object> keysRemoved) {
    EventID eventId = super.distributeTombstoneGC(keysRemoved);
    if (keysRemoved != null && keysRemoved.size() > 0 && getFilterProfile() != null) {
      // send the GC to members that don't have the bucket but have the PR so they
      // can forward the event to clients
      PRTombstoneMessage.send(this, keysRemoved, eventId);
    }
    return eventId;
  }

  @Override
  boolean needsTombstoneGCKeysForClients(EventID eventID, FilterInfo clientRouting) {
    if (eventID == null) {
      return false;
    }
    if (CacheClientNotifier.getInstance() == null) {
      return false;
    }
    if (clientRouting != null) {
      return true;
    }
    return getFilterProfile() != null;
  }

  @Override
  void notifyClientsOfTombstoneGC(Map<VersionSource, Long> regionGCVersions,
      Set<Object> removedKeys, EventID eventID, FilterInfo routing) {
    if (CacheClientNotifier.singletonHasClientProxies()) {
      // Only route the event to clients interested in the partitioned region.
      // We do this by constructing a region-level event and then use it to
      // have the filter profile ferret out all of the clients that have interest
      // in this region
      FilterProfile fp = getFilterProfile();
      // fix for bug #46309 - don't send null/empty key set to clients
      if ((removedKeys != null && !removedKeys.isEmpty()) // bug #51877 - NPE in clients
          && (routing != null || (fp != null && fp.hasInterest()))) {
        RegionEventImpl regionEvent = new RegionEventImpl(getPartitionedRegion(),
            Operation.REGION_DESTROY, null, true, getMyId());
        FilterInfo clientRouting = routing;
        if (clientRouting == null) {
          clientRouting = fp.getLocalFilterRouting(regionEvent);
        }
        regionEvent.setLocalFilterInfo(clientRouting);

        ClientUpdateMessage clientMessage =
            ClientTombstoneMessage.gc(getPartitionedRegion(), removedKeys, eventID);
        CacheClientNotifier.notifyClients(regionEvent, clientMessage);
      }
    }
  }

  /**
   * Search the CM for keys. If found any, return the first found one Otherwise, save the keys into
   * the CM, and return null The thread will acquire the lock before searching.
   *
   * @return first key found in CM null means not found
   */
  LockObject searchAndLock(Object[] keys) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    LockObject foundLock = null;

    synchronized (allKeysMap) {
      // check if there's any key in map
      for (Object key : keys) {
        if (allKeysMap.containsKey(key)) {
          foundLock = allKeysMap.get(key);
          if (isDebugEnabled) {
            logger.debug("LockKeys: found key: {}:{}", key, foundLock.lockedTimeStamp);
          }
          foundLock.waiting();
          break;
        }
      }

      // save the keys when still locked
      if (foundLock == null) {
        for (Object key : keys) {
          LockObject lockValue =
              new LockObject(key, isDebugEnabled ? System.currentTimeMillis() : 0);
          allKeysMap.put(key, lockValue);
          if (isDebugEnabled) {
            logger.debug("LockKeys: add key: {}:{}", key, lockValue.lockedTimeStamp);
          }
        }
      }
    }

    return foundLock;
  }

  /**
   * After processed the keys, this method will remove them from CM. And notifyAll for each key. The
   * thread needs to acquire lock of CM first.
   */
  public void removeAndNotifyKeys(Object[] keys) {
    final boolean isTraceEnabled = logger.isTraceEnabled();

    synchronized (allKeysMap) {
      for (Object key : keys) {
        LockObject lockValue = allKeysMap.remove(key);
        if (lockValue != null) {
          // let current thread become the monitor of the key object
          synchronized (lockValue) {
            lockValue.setRemoved();
            if (isTraceEnabled) {
              long waitTime = System.currentTimeMillis() - lockValue.lockedTimeStamp;
              logger.trace("LockKeys: remove key {}, notifyAll for {}. It waited {}", key,
                  lockValue, waitTime);
            }
            if (lockValue.isSomeoneWaiting()) {
              lockValue.notifyAll();
            }
          }
        }
      } // for
    }
  }

  /**
   * Keep checking if CM has contained any key in keys. If yes, wait for notify, then retry again.
   * This method will block current thread for long time. It only exits when current thread
   * successfully save its keys into CM.
   */
  public boolean waitUntilLocked(Object[] keys) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final String title = "BucketRegion.waitUntilLocked:";
    while (true) {
      LockObject foundLock = searchAndLock(keys);

      if (foundLock != null) {
        synchronized (foundLock) {
          try {
            while (!foundLock.isRemoved()) {
              partitionedRegion.checkReadiness();
              foundLock.wait(1000);
              // primary could be changed by prRebalancing while waiting here
              checkForPrimary();
            }
          } catch (InterruptedException e) {
            // TODO this isn't a localizable string and it's being logged at info level
            if (isDebugEnabled) {
              logger.debug("{} interrupted while waiting for {}", title, foundLock);
            }
          }
          if (isDebugEnabled) {
            long waitTime = System.currentTimeMillis() - foundLock.lockedTimeStamp;
            logger.debug("{} waited {} ms to lock {}", title, waitTime, foundLock);
          }
        }
      } else {
        // now the keys have been locked by this thread
        return true;
      } // to lock and process
    } // while
  }

  // Entry (Put/Create) rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka update or create entry
  // 2) distribute op to bucket secondaries and cache servers with synchrony on local entry
  // 3) cache listener with synchrony on entry
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  public boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModificaiton)
      throws TimeoutException, CacheWriterException {
    boolean locked = lockKeysAndPrimary(event);

    try {
      if (partitionedRegion.isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      if (!hasSeenEvent(event)) {
        forceSerialized(event);
        RegionEntry oldEntry = entries.basicPut(event, lastModified, ifNew, ifOld,
            expectedOldValue, requireOldValue, overwriteDestroyed);
        return oldEntry != null;
      }
      if (event.getDeltaBytes() != null && event.getRawNewValue() == null) {
        // This means that this event has delta bytes but no full value.
        // Request the full value of this event.
        // The value in this vm may not be same as this event's value.
        throw new InvalidDeltaException(
            "Cache encountered replay of event containing delta bytes for key " + event.getKey());
      }
      // Forward the operation and event messages
      // to members with bucket copies that may not have seen the event. Their
      // EventTrackers will keep them from applying the event a second time if
      // they've already seen it.
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "BR.virtualPut: this cache has already seen this event {}", event);
      }
      if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
        distributeUpdateOperation(event, lastModified);
      }
      return true;
    } finally {
      if (locked) {
        releaseLockForKeysAndPrimary(event);
      }
    }
  }


  long generateTailKey() {
    long key = eventSeqNum.addAndGet(partitionedRegion.getTotalNumberOfBuckets());
    if (key < 0 || key % getPartitionedRegion().getTotalNumberOfBuckets() != getId()) {
      logger.error("ERROR! The sequence number {} generated for the bucket {} is incorrect.",
          new Object[] {key, getId()});
    }
    if (logger.isDebugEnabled()) {
      logger.debug("WAN: On primary bucket {}, setting the seq number as {}", getId(),
          eventSeqNum.get());
    }
    return eventSeqNum.get();
  }

  @Override
  public void handleWANEvent(EntryEventImpl event) {
    if (eventSeqNum == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The bucket corresponding to this user bucket is not created yet. This event will not go to remote wan site. Event: {}",
            event);
      }
    }

    if (!(this instanceof AbstractBucketRegionQueue)) {
      if (getBucketAdvisor().isPrimary()) {
        long key = eventSeqNum.addAndGet(partitionedRegion.getTotalNumberOfBuckets());
        if (key < 0 || key % getPartitionedRegion().getTotalNumberOfBuckets() != getId()) {
          logger.error("ERROR! The sequence number {} generated for the bucket {} is incorrect.",
              new Object[] {key, getId()});
        }
        event.setTailKey(key);
        if (logger.isDebugEnabled()) {
          logger.debug("WAN: On primary bucket {}, setting the seq number as {}", getId(),
              eventSeqNum.get());
        }
      } else {
        // Can there be a race here? Like one thread has done put in primary but
        // its update comes later
        // in that case its possible that a tail key is missed.
        // we can handle that by only incrementing the tailKey and never
        // setting it less than the current value.
        Atomics.setIfGreater(eventSeqNum, event.getTailKey());
        if (logger.isDebugEnabled()) {
          logger.debug("WAN: On secondary bucket {}, setting the seq number as {}", getId(),
              event.getTailKey());
        }
      }
    }
  }

  /**
   * Fix for Bug#45917 We are updating the seqNumber so that new seqNumbers are generated starting
   * from the latest in the system.
   */
  void updateEventSeqNum(long l) {
    Atomics.setIfGreater(eventSeqNum, l);
    if (logger.isDebugEnabled()) {
      logger.debug("WAN: On bucket {}, setting the seq number as {} before GII", getId(), l);
    }
  }

  protected void distributeUpdateOperation(EntryEventImpl event, long lastModified) {
    long token = -1;
    UpdateOperation op = null;

    try {
      if (!event.isOriginRemote() && !event.isNetSearch() && getBucketAdvisor().isPrimary()) {
        if (event.isBulkOpInProgress()) {
          // consolidate the UpdateOperation for each entry into a PutAllMessage
          // since we did not call basicPutPart3(), so we have to explicitly addEntry here
          event.getPutAllOperation().addEntry(event, getId());
        } else {
          // before distribute: BR's put
          op = new UpdateOperation(event, lastModified);
          token = op.startOperation();
          if (logger.isDebugEnabled()) {
            logger.debug("sent update operation : for region  : {}: with event: {}", getName(),
                event);
          }
        }
      }
      if (!event.getOperation().isPutAll()) { // putAll will invoke listeners later
        event.invokeCallbacks(this, true, true);
      }
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  /**
   * distribute the operation in basicPutPart2 so the region entry lock is held
   */
  @Override
  public long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
      long lastModified, boolean clearConflict) {
    // Assumed this is called with entry synchrony

    // Typically UpdateOperation is called with the
    // timestamp returned from basicPutPart2, but as a bucket we want to do
    // distribution *before* we do basicPutPart2.
    final long modifiedTime = event.getEventTime(lastModified);

    long token = -1;
    UpdateOperation op = null;

    try {
      // Update the get stats if necessary.
      if (partitionedRegion.getDataStore().hasClientInterest(event)) {
        updateStatsForGet(entry, true);
      }
      if (!event.isOriginRemote()) {
        if (event.getVersionTag() == null || event.getVersionTag().isGatewayTag()) {
          boolean eventHasDelta = event.getDeltaBytes() != null;
          VersionTag v = entry.generateVersionTag(null, eventHasDelta, this, event);
          if (v != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("generated version tag {} in region {}", v, getName());
            }
          }
        }

        // This code assumes it is safe ignore token mode (GII in progress)
        // because it assumes when the origin of the event is local,
        // the GII has completed and the region is initialized and open for local
        // ops

        if (!event.isBulkOpInProgress()) {
          long start = partitionedRegion.getPrStats().startSendReplication();
          try {
            // before distribute: PR's put PR
            op = new UpdateOperation(event, modifiedTime);
            token = op.startOperation();
          } finally {
            partitionedRegion.getPrStats().endSendReplication(start);
          }
        } else {
          // consolidate the UpdateOperation for each entry into a PutAllMessage
          // basicPutPart3 takes care of this
        }
      }

      return super.basicPutPart2(event, entry, isInitialized, lastModified, clearConflict);
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  @Override
  protected void notifyGatewaySender(EnumListenerEvent operation, EntryEventImpl event) {
    // We don't need to clone the event for new Gateway Senders.
    // Preserve the bucket reference for resetting it later.
    InternalRegion bucketRegion = event.getRegion();
    try {
      event.setRegion(partitionedRegion);
      partitionedRegion.notifyGatewaySender(operation, event);
    } finally {
      // reset the event region back to bucket region.
      // This should work as gateway queue create GatewaySenderEvent for
      // queueing.
      event.setRegion(bucketRegion);
    }
  }

  public void checkForPrimary() {
    final boolean isp = getBucketAdvisor().isPrimary();
    if (!isp) {
      partitionedRegion.checkReadiness();
      checkReadiness();
      InternalDistributedMember primaryHolder = getBucketAdvisor().basicGetPrimaryMember();
      throw new PrimaryBucketException(
          "Bucket " + getName() + " is not primary. Current primary holder is " + primaryHolder);
    }
  }

  /**
   * Checks to make sure that this node is primary, and locks the bucket to make sure the bucket
   * stays the primary bucket while the write is in progress. This method must be followed with
   * a call to releaseLockForKeysAndPrimary() if keys and primary are locked.
   */
  boolean lockKeysAndPrimary(EntryEventImpl event) {
    if (!needWriteLock(event)) {
      return false;
    }

    if (cache.isCacheAtShutdownAll()) {
      throw cache.getCacheClosedException("Cache is shutting down");
    }

    Object[] keys = getKeysToBeLocked(event);
    waitUntilLocked(keys); // it might wait for long time

    boolean lockedForPrimary = false;
    try {
      lockedForPrimary = doLockForPrimary(false);
      // tryLock is false means doLockForPrimary won't return false.
      // either the method returns true or fails with an exception
      assert lockedForPrimary : "expected doLockForPrimary returns true";
      return lockedForPrimary;
    } finally {
      if (!lockedForPrimary) {
        removeAndNotifyKeys(keys);
      }
    }
  }

  Object[] getKeysToBeLocked(EntryEventImpl event) {
    Object[] keys = new Object[1];
    keys[0] = event.getKey();
    return keys;
  }

  /**
   * lock this bucket and, if present, its colocated "parent"
   *
   * @param tryLock - whether to use tryLock (true) or a blocking lock (false)
   * @return true if locks were obtained and are still held
   */
  public boolean doLockForPrimary(boolean tryLock) {
    boolean locked = lockPrimaryStateReadLock(tryLock);
    if (!locked) {
      return false;
    }

    boolean isPrimary = false;
    try {
      // Throw a PrimaryBucketException if this VM is assumed to be the
      // primary but isn't, preventing update and distribution
      checkForPrimary();

      if (cache.isCacheAtShutdownAll()) {
        throw cache.getCacheClosedException("Cache is shutting down");
      }

      isPrimary = true;
    } finally {
      if (!isPrimary) {
        doUnlockForPrimary();
      }
    }

    return true;
  }

  private boolean lockPrimaryStateReadLock(boolean tryLock) {
    Lock primaryMoveReadLock = getBucketAdvisor().getPrimaryMoveReadLock();
    Lock parentLock = getBucketAdvisor().getParentPrimaryMoveReadLock();
    for (;;) {
      boolean interrupted = Thread.interrupted();
      try {
        // Get the lock. If we have to wait here, it's because
        // this VM is actively becoming "not primary". We don't want
        // to throw an exception until this VM is actually no longer
        // primary, so we wait here for not primary to complete. See bug #39963
        if (parentLock != null) {
          if (tryLock) {
            boolean locked = parentLock.tryLock();
            if (!locked) {
              return false;
            }
          } else {
            parentLock.lockInterruptibly();
          }
          if (tryLock) {
            boolean locked = primaryMoveReadLock.tryLock();
            if (!locked) {
              parentLock.unlock();
              return false;
            }
          } else {
            primaryMoveReadLock.lockInterruptibly();
          }
        } else {
          if (tryLock) {
            boolean locked = primaryMoveReadLock.tryLock();
            if (!locked) {
              return false;
            }
          } else {
            primaryMoveReadLock.lockInterruptibly();
          }
        }
        break; // success
      } catch (InterruptedException e) {
        interrupted = true;
        cache.getCancelCriterion().checkCancelInProgress(null);
        // don't throw InternalGemFireError to fix bug 40102
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    return true;
  }

  public void doUnlockForPrimary() {
    Lock primaryMoveReadLock = getBucketAdvisor().getPrimaryMoveReadLock();
    primaryMoveReadLock.unlock();
    Lock parentLock = getBucketAdvisor().getParentPrimaryMoveReadLock();
    if (parentLock != null) {
      parentLock.unlock();
    }
  }

  /**
   * Release the lock on the bucket that makes the bucket stay the primary during a write.
   * And release/remove the lockObject on the key(s)
   */
  void releaseLockForKeysAndPrimary(EntryEventImpl event) {
    doUnlockForPrimary();

    Object[] keys = getKeysToBeLocked(event);
    removeAndNotifyKeys(keys);
  }

  protected boolean needWriteLock(EntryEventImpl event) {
    return !(event.isOriginRemote() || event.isNetSearch() || event.getOperation().isLocal()
        || event.getOperation().isPutAll() || event.getOperation().isRemoveAll()
        || (event.isExpiration() && isEntryEvictDestroyEnabled()
            || event.isPendingSecondaryExpireDestroy()));
  }

  // this is stubbed out because distribution is done in basicPutPart2 while
  // the region entry is still locked
  @Override
  protected void distributeUpdate(EntryEventImpl event, long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue) {}

  // Entry Invalidation rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka update entry
  // 2) distribute op to bucket secondaries and cache servers with synchrony on local entry
  // 3) cache listener with synchrony on entry
  // 4) update local bs, gateway
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  public void basicInvalidate(EntryEventImpl event) throws EntryNotFoundException {
    basicInvalidate(event, isInitialized(), false);
  }

  @Override
  void basicInvalidate(final EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry)
      throws EntryNotFoundException {
    // disallow local invalidation
    Assert.assertTrue(!event.isLocalInvalid());
    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    boolean locked = lockKeysAndPrimary(event);
    try {
      // which performs the local op.
      // The ARM then calls basicInvalidatePart2 with the entry synchronized.
      if (!hasSeenEvent(event)) {
        if (event.getOperation().isExpiration()) { // bug 39905 - invoke listeners for expiration
          DistributedSystem sys = cache.getDistributedSystem();
          EventID newID = new EventID(sys);
          event.setEventId(newID);
          event.setInvokePRCallbacks(getBucketAdvisor().isPrimary());
        }
        boolean forceCallbacks = isEntryEvictDestroyEnabled();
        boolean done =
            entries.invalidate(event, invokeCallbacks, forceNewEntry, forceCallbacks);
        ExpirationAction expirationAction = getEntryExpirationAction();
        if (done && !getBucketAdvisor().isPrimary() && expirationAction != null
            && expirationAction.isInvalidate()) {
          synchronized (pendingSecondaryExpires) {
            pendingSecondaryExpires.remove(event.getKey());
          }
        }
      } else {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "LR.basicInvalidate: this cache has already seen this event {}", event);
        }
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeInvalidateOperation(event);
        }
      }
    } finally {
      if (locked) {
        releaseLockForKeysAndPrimary(event);
      }
    }
  }

  protected void distributeInvalidateOperation(EntryEventImpl event) {
    InvalidateOperation op = null;
    long token = -1;
    try {
      if (!event.isOriginRemote() && getBucketAdvisor().isPrimary()) {
        // This cache has processed the event, forward operation
        // and event messages to backup buckets
        // before distribute: BR.invalidate hasSeenEvent
        op = new InvalidateOperation(event);
        token = op.startOperation();
      }
      event.invokeCallbacks(this, true, false);
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  @Override
  void basicInvalidatePart2(final RegionEntry regionEntry, final EntryEventImpl event,
      boolean conflictWithClear, boolean invokeCallbacks) {
    // Assumed this is called with the entry synchronized
    long token = -1;
    InvalidateOperation op = null;

    try {
      if (!event.isOriginRemote()) {
        if (event.getVersionTag() == null || event.getVersionTag().isGatewayTag()) {
          VersionTag v = regionEntry.generateVersionTag(null, false, this, event);
          if (logger.isDebugEnabled() && v != null) {
            logger.debug("generated version tag {} in region {}", v, getName());
          }
          event.setVersionTag(v);
        }

        // This code assumes it is safe ignore token mode (GII in progress)
        // because it assumes when the origin of the event is local,
        // the GII has completed and the region is initialized and open for local
        // ops

        // This code assumes that this bucket is primary
        // distribute op to bucket secondaries and event to other listeners
        // before distribute: BR's invalidate
        op = new InvalidateOperation(event);
        token = op.startOperation();
      }
      super.basicInvalidatePart2(regionEntry, event,
          conflictWithClear /* Clear conflict occurred */, invokeCallbacks);
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  @Override
  void distributeInvalidate(EntryEventImpl event) {}

  @Override
  protected void distributeInvalidateRegion(RegionEventImpl event) {
    // switch region in event so that we can have distributed region
    // send InvalidateRegion message.
    event.region = this;
    super.distributeInvalidateRegion(event);
    event.region = partitionedRegion;
  }

  @Override
  protected boolean shouldDistributeInvalidateRegion(RegionEventImpl event) {
    return getBucketAdvisor().isPrimary();
  }

  @Override
  boolean shouldGenerateVersionTag(RegionEntry entry, EntryEventImpl event) {
    if (event.getOperation().isLocal()) { // bug #45402 - localDestroy generated a version tag
      return false;
    }
    return getConcurrencyChecksEnabled()
        && ((event.getVersionTag() == null) || event.getVersionTag().isGatewayTag());
  }

  @Override
  void expireDestroy(EntryEventImpl event, boolean cacheWrite) {

    /* Early out before we throw a PrimaryBucketException because we're not primary */
    if (needWriteLock(event) && !getBucketAdvisor().isPrimary()) {
      return;
    }
    try {
      super.expireDestroy(event, cacheWrite);
    } catch (PrimaryBucketException ignored) {
      // must have concurrently removed the primary
    }
  }

  @Override
  void expireInvalidate(EntryEventImpl event) {
    if (!getBucketAdvisor().isPrimary()) {
      return;
    }
    try {
      super.expireInvalidate(event);
    } catch (PrimaryBucketException e) {
      // must have concurrently removed the primary
    }
  }

  @Override
  void performExpiryTimeout(ExpiryTask expiryTask) throws CacheException {
    boolean isEvictDestroy = isEntryEvictDestroyEnabled();
    // Fix for bug 43805 - get the primary lock before
    // synchronizing on pendingSecondaryExpires, to match the lock
    // ordering in other place (like acquiredPrimaryLock)
    lockPrimaryStateReadLock(false);
    try {
      // Why do we care if evict destroy is configured?
      // See bug 41096 for the answer.
      if (!getBucketAdvisor().isPrimary() && !isEvictDestroy) {
        synchronized (pendingSecondaryExpires) {
          if (expiryTask.isPending()) {
            Object key = expiryTask.getKey();
            if (key != null) {
              pendingSecondaryExpires.put(key, expiryTask);
            }
          }
        }
      } else {
        super.performExpiryTimeout(expiryTask);
      }
    } finally {
      doUnlockForPrimary();
    }
  }

  private boolean isEntryEvictDestroyEnabled() {
    return getEvictionAttributes() != null
        && EvictionAction.LOCAL_DESTROY.equals(getEvictionAttributes().getAction());
  }

  void processPendingSecondaryExpires() {
    ExpiryTask[] tasks;
    while (true) {
      // note we just keep looping until no more pendingExpires exist
      synchronized (pendingSecondaryExpires) {
        if (pendingSecondaryExpires.isEmpty()) {
          return;
        }
        tasks = new ExpiryTask[pendingSecondaryExpires.size()];
        tasks = pendingSecondaryExpires.values().toArray(tasks);
        pendingSecondaryExpires.clear();
      }
      try {
        if (isCacheClosing() || isClosed() || isDestroyed) {
          return;
        }
        final boolean isDebugEnabled = logger.isDebugEnabled();
        for (ExpiryTask task : tasks) {
          try {
            if (isDebugEnabled) {
              logger.debug("{} fired at {}", task, System.currentTimeMillis());
            }
            task.basicPerformTimeout(true);
            if (isCacheClosing() || isClosed() || isDestroyed()) {
              return;
            }
          } catch (EntryNotFoundException ignore) {
            // ignore and try the next expiry task
          }
        }
      } catch (RegionDestroyedException ignored) {
        // Ignore - our job is done
      } catch (CancelException ignored) {
        // ignore
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable ex) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.fatal("Exception in expiration task", ex);
      }
    }
  }

  /**
   * Creates an event for the EVICT_DESTROY operation so that events will fire for Partitioned
   * Regions.
   *
   * @param key - the key that this event is related to
   * @return an event for EVICT_DESTROY
   */
  @Override
  @Retained
  EntryEventImpl generateEvictDestroyEvent(Object key) {
    EntryEventImpl event = super.generateEvictDestroyEvent(key);
    event.setInvokePRCallbacks(true); // see bug 40797
    return event;
  }

  // Entry Destruction rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka destroy entry (REMOVED token)
  // 2) distribute op to bucket secondaries and cache servers with synchrony on local entry
  // 3) cache listener with synchrony on local entry
  // 4) update local bs, gateway
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  public void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
      Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {

    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    boolean locked = lockKeysAndPrimary(event);
    try {
      // increment the tailKey for the destroy event
      if (partitionedRegion.isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      // This call should invoke AbstractRegionMap (aka ARM) destroy method
      // which calls the CacheWriter, then performs the local op.
      // The ARM then calls basicDestroyPart2 with the entry synchronized.
      if (!hasSeenEvent(event)) {
        if (event.getOperation().isExpiration()) { // bug 39905 - invoke listeners for expiration
          DistributedSystem sys = cache.getDistributedSystem();
          if (event.getEventId() == null) { // Fix for #47388
            EventID newID = new EventID(sys);
            event.setEventId(newID);
          }
          event.setInvokePRCallbacks(getBucketAdvisor().isPrimary());
        }
        boolean done = mapDestroy(event, cacheWrite, false, // isEviction //merge44610: In cheetah
                                                            // instead of false
                                                            // event.getOperation().isEviction() is
                                                            // used. We kept the cedar change as it
                                                            // is.
            expectedOldValue);
        if (done && !getBucketAdvisor().isPrimary() && isEntryExpiryPossible()) {
          synchronized (pendingSecondaryExpires) {
            pendingSecondaryExpires.remove(event.getKey());
          }
        }
      } else {
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeDestroyOperation(event);
        }
      }
    } finally {
      if (locked) {
        releaseLockForKeysAndPrimary(event);
      }
    }
  }

  protected void distributeDestroyOperation(EntryEventImpl event) {
    long token = -1;
    DestroyOperation op = null;

    try {
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "BR.basicDestroy: this cache has already seen this event {}", event);
      }
      if (!event.isOriginRemote() && getBucketAdvisor().isPrimary()) {
        if (event.isBulkOpInProgress()) {
          // consolidate the DestroyOperation for each entry into a RemoveAllMessage
          event.getRemoveAllOperation().addEntry(event, getId());
        } else {
          // This cache has processed the event, forward operation
          // and event messages to backup buckets
          // before distribute: BR's destroy, not to trigger callback here
          event.setOldValueFromRegion();
          op = new DestroyOperation(event);
          token = op.startOperation();
        }
      }

      if (!event.getOperation().isRemoveAll()) { // removeAll will invoke listeners later
        event.invokeCallbacks(this, true, false);
      }
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  @Override
  public void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event) {
    long token = -1;
    DestroyOperation op = null;
    try {
      // Assumed this is called with entry synchrony
      if (!event.isOriginRemote() && !event.isBulkOpInProgress() && !event.getOperation().isLocal()
          && !Operation.EVICT_DESTROY.equals(event.getOperation())
          && !(event.isExpiration() && isEntryEvictDestroyEnabled())) {

        if (event.getVersionTag() == null || event.getVersionTag().isGatewayTag()) {
          VersionTag v = entry.generateVersionTag(null, false, this, event);
          if (logger.isDebugEnabled() && v != null) {
            logger.debug("generated version tag {} in region {}", v, getName());
          }
        }

        // This code assumes it is safe ignore token mode (GII in progress)
        // because it assume when the origin of the event is local,
        // then GII has completed (the region has been completely initialized)

        // This code assumes that this bucket is primary
        // before distribute: BR.destroy for retain
        op = new DestroyOperation(event);
        token = op.startOperation();
      }
      super.basicDestroyBeforeRemoval(entry, event);
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  @Override
  void distributeDestroy(EntryEventImpl event, Object expectedOldValue) {}


  // impl removed - not needed for listener invocation alterations
  // void basicDestroyPart2(RegionEntry re, EntryEventImpl event, boolean inTokenMode, boolean
  // invokeCallbacks)

  @Override
  void validateArguments(Object key, Object value, Object aCallbackArgument) {
    Assert.assertTrue(!isTX());
    super.validateArguments(key, value, aCallbackArgument);
  }

  public void forceSerialized(EntryEventImpl event) {
    event.makeSerializedNewValue();
    // Object obj = event.getRawNewValue();
    // if (obj instanceof byte[]
    // || obj == null
    // || obj instanceof CachedDeserializable
    // || obj == NotAvailable.NOT_AVAILABLE
    // || Token.isInvalidOrRemoved(obj)) {
    // // already serialized
    // return;
    // }
    // throw new InternalGemFireError("event did not force serialized: " + event);
  }

  /**
   * This method is called when a miss from a get ends up finding an object through a cache loader
   * or from a server. In that case we want to make sure that we don't move this bucket while
   * putting the value in the ache.
   *
   * @see LocalRegion#basicPutEntry(EntryEventImpl, long)
   */
  @Override
  RegionEntry basicPutEntry(final EntryEventImpl event, final long lastModified)
      throws TimeoutException, CacheWriterException {
    boolean locked = lockKeysAndPrimary(event);
    try {
      if (getPartitionedRegion().isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      event.setInvokePRCallbacks(true);
      forceSerialized(event);
      return super.basicPutEntry(event, lastModified);
    } finally {
      if (locked) {
        releaseLockForKeysAndPrimary(event);
      }
    }
  }

  @Override
  void basicUpdateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {

    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    InternalRegion internalRegion = event.getRegion();
    AbstractRegionMap arm = ((AbstractRegionMap) internalRegion.getRegionMap());

    arm.lockForCacheModification(internalRegion, event);
    final boolean locked = internalRegion.lockWhenRegionIsInitializing();
    try {
      boolean keysAndPrimaryLocked = lockKeysAndPrimary(event);
      try {
        if (!hasSeenEvent(event)) {
          entries.updateEntryVersion(event);
        } else {
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE,
                "BR.basicUpdateEntryVersion: this cache has already seen this event {}", event);
          }
        }
        if (!event.isOriginRemote() && getBucketAdvisor().isPrimary()) {
          // This cache has processed the event, forward operation
          // and event messages to backup buckets
          if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
            distributeUpdateEntryVersionOperation(event);
          }
        }
      } finally {
        if (keysAndPrimaryLocked) {
          releaseLockForKeysAndPrimary(event);
        }
      }
    } finally {
      if (locked) {
        internalRegion.unlockWhenRegionIsInitializing();
      }
      arm.releaseCacheModificationLock(event.getRegion(), event);
    }
  }

  protected void distributeUpdateEntryVersionOperation(EntryEventImpl event) {
    new UpdateEntryVersionOperation(event).distribute();
  }

  public int getRedundancyLevel() {
    return redundancy;
  }

  @Override
  public boolean isPrimary() {
    throw new UnsupportedOperationException(
        String.format("This should never be called on %s", getClass()));
  }

  @Override
  public boolean isDestroyed() {
    // TODO prpersist - Added this if null check for the partitioned region
    // because we create the disk store for a bucket *before* in the constructor
    // for local region, which is before this final field is assigned. This is why
    // we shouldn't do some much work in the constructors! This is a temporary
    // hack until I move must of the constructor code to region.initialize.
    return isBucketDestroyed() || (partitionedRegion != null
        && partitionedRegion.isLocallyDestroyed && !isInDestroyingThread());
  }

  /**
   * Return true if this bucket has been destroyed. Don't bother checking to see if the PR that owns
   * this bucket was destroyed; that has already been checked.
   *
   * @since GemFire 6.0
   */
  public boolean isBucketDestroyed() {
    return super.isDestroyed();
  }

  @Override
  int sizeEstimate() {
    return size();
  }

  @Override
  public void checkReadiness() {
    super.checkReadiness();
    if (isDestroyed()) {
      throw new RegionDestroyedException(toString(), getFullPath());
    }
  }

  @Override
  public PartitionedRegion getPartitionedRegion() {
    return partitionedRegion;
  }

  /**
   * is the current thread involved in destroying the PR that owns this region?
   */
  private boolean isInDestroyingThread() {
    return partitionedRegion.locallyDestroyingThread == Thread.currentThread();
  }

  @Override
  public void fillInProfile(Profile profile) {
    super.fillInProfile(profile);
    BucketProfile bp = (BucketProfile) profile;
    bp.isInitializing = getInitializationLatchAfterGetInitialImage().getCount() > 0;
  }

  /** check to see if the partitioned region is locally destroyed or closed */
  boolean isPartitionedRegionOpen() {
    return !partitionedRegion.isLocallyDestroyed && !partitionedRegion.isClosed
        && !partitionedRegion.isDestroyed();
  }

  /**
   * Horribly plagiarized from the similar method in LocalRegion
   *
   * @param clientEvent holder for client version tag
   * @param returnTombstones whether Token.TOMBSTONE should be returned for destroyed entries
   * @return serialized form if present, null if the entry is not in the cache, or INVALID or
   *         LOCAL_INVALID re is a miss (invalid)
   */
  private RawValue getSerialized(Object key, boolean updateStats, boolean doNotLockEntry,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws EntryNotFoundException {
    RegionEntry re;
    re = entries.getEntry(key);
    if (re == null) {
      return NULLVALUE;
    }
    if (re.isTombstone() && !returnTombstones) {
      return NULLVALUE;
    }
    Object v;

    try {
      v = re.getValue(this);
      if (doNotLockEntry) {
        if (v == Token.NOT_AVAILABLE || v == null) {
          return REQUIRES_ENTRY_LOCK;
        }
      }
      if (clientEvent != null) {
        VersionStamp stamp = re.getVersionStamp();
        if (stamp != null) {
          clientEvent.setVersionTag(stamp.asVersionTag());
        }
      }
    } catch (DiskAccessException dae) {
      handleDiskAccessException(dae);
      throw dae;
    }

    if (v == null) {
      return NULLVALUE;
    } else {
      if (updateStats) {
        updateStatsForGet(re, true);
      }
      return new RawValue(v);
    }
  }

  /**
   * Return serialized form of an entry
   * <p>
   * Horribly plagiarized from the similar method in LocalRegion
   *
   * @param clientEvent holder for the entry's version information
   * @return serialized (byte) form
   * @throws IOException if the result is not serializable
   * @see LocalRegion#get(Object, Object, boolean, EntryEventImpl)
   */
  public RawValue getSerialized(KeyInfo keyInfo, boolean generateCallbacks, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws IOException {
    checkReadiness();
    checkForNoAccess();
    CachePerfStats stats = getCachePerfStats();
    long start = stats.startGet();

    boolean miss = true;
    try {
      boolean isCreate;
      RawValue result =
          getSerialized(keyInfo.getKey(), true, doNotLockEntry, clientEvent, returnTombstones);
      isCreate =
          result == NULLVALUE || (result.getRawValue() == Token.TOMBSTONE && !returnTombstones);
      miss = (result == NULLVALUE || Token.isInvalid(result.getRawValue()));
      if (miss) {
        // if scope is local and there is no loader, then
        // don't go further to try and get value
        if (hasServerProxy() || basicGetLoader() != null) {
          if (doNotLockEntry) {
            return REQUIRES_ENTRY_LOCK;
          }
          Object value = nonTxnFindObject(keyInfo, isCreate, generateCallbacks,
              result.getRawValue(), true, true, requestingClient, clientEvent, false);
          if (value != null) {
            result = new RawValue(value);
          }
        } else { // local scope with no loader, still might need to update stats
          if (isCreate) {
            recordMiss(null, keyInfo.getKey());
          }
        }
      }
      return result; // changed in 7.0 to return RawValue(Token.INVALID) if the entry is invalid
    } finally {
      stats.endGet(start, miss);
    }

  } // getSerialized

  @Override
  public String toString() {
    return "BucketRegion" + "[path='" + getFullPath()
        + ";serial=" + getSerialNumber() + ";primary="
        + getBucketAdvisor().getProxyBucketRegion().isPrimary() + "]";
  }

  @Override
  protected void distributedRegionCleanup(RegionEventImpl event) {
    // No need to close advisor, assume its already closed
    // However we need to remove our listener from the advisor (see bug 43950).
    distAdvisor.removeMembershipListener(advisorListener);
  }

  /**
   * Tell the peers that this VM has destroyed the region.
   *
   * Also marks the local disk files as to be deleted before sending the message to peers.
   *
   *
   * @param rebalance true if this is due to a rebalance removing the bucket
   */
  void removeFromPeersAdvisors(boolean rebalance) {
    if (getPersistenceAdvisor() != null) {
      getPersistenceAdvisor().releaseTieLock();
    }

    DiskRegion diskRegion = getDiskRegion();

    // Tell our peers whether we are destroying this region
    // or just closing it.
    boolean shouldDestroy = rebalance || diskRegion == null || !diskRegion.isRecreated();
    Operation op = shouldDestroy ? Operation.REGION_LOCAL_DESTROY : Operation.REGION_CLOSE;

    RegionEventImpl event = new RegionEventImpl(this, op, null, false, getMyId(),
        generateEventID()/* generate EventID */);
    // When destroying the whole partitioned region, there's no need to
    // distribute the region closure/destruction, the PR RegionAdvisor.close()
    // has taken care of it
    if (isPartitionedRegionOpen()) {


      // Only delete the files on the local disk if
      // this is a rebalance, or we are creating the bucket
      // for the first time
      if (diskRegion != null && shouldDestroy) {
        diskRegion.beginDestroyDataStorage();
      }

      // Send out the destroy op to peers
      new DestroyRegionOperation(event, true).distribute();
    }
  }

  @Override
  void distributeDestroyRegion(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    // No need to do this when we actually destroy the region,
    // we already distributed this info.
  }

  @Retained
  EntryEventImpl createEventForPR(EntryEventImpl sourceEvent) {
    EntryEventImpl e2 = new EntryEventImpl(sourceEvent);
    boolean returned = false;
    try {
      e2.setRegion(partitionedRegion);
      if (FORCE_LOCAL_LISTENERS_INVOCATION) {
        e2.setInvokePRCallbacks(true);
      } else {
        e2.setInvokePRCallbacks(sourceEvent.getInvokePRCallbacks());
      }
      DistributedMember dm = getDistributionManager().getDistributionManagerId();
      e2.setOriginRemote(!e2.getDistributedMember().equals(dm));
      returned = true;
      return e2;
    } finally {
      if (!returned) {
        e2.release();
      }
    }
  }

  private boolean skipPrEvent(final EntryEventImpl event, final boolean callDispatchListenerEvent) {
    if (!event.isGenerateCallbacks()) {
      return true;
    }
    boolean needsPrEvent = (partitionedRegion.isInitialized() && callDispatchListenerEvent
        && partitionedRegion.shouldDispatchListenerEvent())
        || CacheClientNotifier.singletonHasClientProxies();
    if (!needsPrEvent) {
      return true;
    }
    return false;
  }

  @Override
  public void invokeTXCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent) {
    if (logger.isDebugEnabled()) {
      logger.debug("BR.invokeTXCallbacks for event {}", event);
    }
    // bucket events may make it to this point even though the bucket is still
    // initializing. We can't block while initializing or a GII state flush
    // may hang, so we avoid notifying the bucket
    if (isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeTXCallbacks(eventType, event, callThem);
    }

    if (skipPrEvent(event, callDispatchListenerEvent)) {
      return;
    }

    @Released
    final EntryEventImpl prEvent = createEventForPR(event);
    try {
      partitionedRegion.invokeTXCallbacks(eventType, prEvent,
          partitionedRegion.isInitialized() && callDispatchListenerEvent);
    } finally {
      prEvent.release();
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.LocalRegion#invokeDestroyCallbacks(org.apache.geode.internal.
   * cache.EnumListenerEvent, org.apache.geode.internal.cache.EntryEventImpl, boolean)
   */
  @Override
  public void invokeDestroyCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways) {
    // bucket events may make it to this point even though the bucket is still
    // initializing. We can't block while initializing or a GII state flush
    // may hang, so we avoid notifying the bucket
    if (isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeDestroyCallbacks(eventType, event, callThem, notifyGateways);
    }

    if (skipPrEvent(event, callDispatchListenerEvent)) {
      return;
    }

    @Released
    final EntryEventImpl prEvent = createEventForPR(event);
    try {
      partitionedRegion.invokeDestroyCallbacks(eventType, prEvent,
          partitionedRegion.isInitialized() && callDispatchListenerEvent, false);
    } finally {
      prEvent.release();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.LocalRegion#invokeInvalidateCallbacks(org.apache.geode.internal
   * .cache.EnumListenerEvent, org.apache.geode.internal.cache.EntryEventImpl, boolean)
   */
  @Override
  public void invokeInvalidateCallbacks(final EnumListenerEvent eventType,
      final EntryEventImpl event, final boolean callDispatchListenerEvent) {
    // bucket events may make it to this point even though the bucket is still
    // initializing. We can't block while initializing or a GII state flush
    // may hang, so we avoid notifying the bucket
    if (isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeInvalidateCallbacks(eventType, event, callThem);
    }

    if (skipPrEvent(event, callDispatchListenerEvent)) {
      return;
    }

    @Released
    final EntryEventImpl prEvent = createEventForPR(event);
    try {
      partitionedRegion.invokeInvalidateCallbacks(eventType, prEvent,
          partitionedRegion.isInitialized() && callDispatchListenerEvent);
    } finally {
      prEvent.release();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.LocalRegion#invokePutCallbacks(org.apache.geode.internal.cache.
   * EnumListenerEvent, org.apache.geode.internal.cache.EntryEventImpl, boolean)
   */
  @Override
  public void invokePutCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways) {
    if (logger.isTraceEnabled()) {
      logger.trace("invoking put callbacks on bucket for event {}", event);
    }
    // bucket events may make it to this point even though the bucket is still
    // initializing. We can't block while initializing or a GII state flush
    // may hang, so we avoid notifying the bucket
    if (isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (callThem && event.isPossibleDuplicate()
          && getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokePutCallbacks(eventType, event, callThem, notifyGateways);
    }

    if (skipPrEvent(event, callDispatchListenerEvent)) {
      return;
    }

    @Released
    final EntryEventImpl prEvent = createEventForPR(event);
    try {
      partitionedRegion.invokePutCallbacks(eventType, prEvent,
          partitionedRegion.isInitialized() && callDispatchListenerEvent, false);
    } finally {
      prEvent.release();
    }
  }

  /**
   * perform adjunct messaging for the given operation
   *
   * @param event the event causing this messaging
   * @param cacheOpRecipients set of receiver which got cacheUpdateOperation.
   * @param adjunctRecipients recipients that must unconditionally get the event
   * @param filterRoutingInfo routing information for all members having the region
   * @param processor the reply processor, or null if there isn't one
   */
  void performAdjunctMessaging(EntryEventImpl event, Set cacheOpRecipients,
      Set adjunctRecipients, FilterRoutingInfo filterRoutingInfo,
      DirectReplyProcessor processor,
      boolean calculateDelta, boolean sendDeltaWithFullValue) {

    PartitionMessage msg = event.getPartitionMessage();
    if (calculateDelta) {
      setDeltaIfNeeded(event);
    }
    if (msg != null) {
      // The primary bucket member which is being modified remotely by a
      // thread via a received PartitionedMessage
      msg = msg.getMessageForRelayToListeners(event, adjunctRecipients);
      msg.setSender(partitionedRegion.getDistributionManager().getDistributionManagerId());
      msg.setSendDeltaWithFullValue(sendDeltaWithFullValue);

      msg.relayToListeners(cacheOpRecipients, adjunctRecipients, filterRoutingInfo,
          event, partitionedRegion, processor);
    } else {
      // The primary bucket is being modified locally by an application thread locally
      Operation op = event.getOperation();
      if (op.isCreate() || op.isUpdate()) {
        // note that at this point ifNew/ifOld have been used to update the
        // local store, and the event operation should be correct
        PutMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, partitionedRegion, event, op.isCreate(), !op.isCreate(),
            processor, sendDeltaWithFullValue);
      } else if (op.isDestroy()) {
        DestroyMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, partitionedRegion, event, processor);
      } else if (op.isInvalidate()) {
        InvalidateMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, partitionedRegion, event, processor);
      }
    }
  }

  private void setDeltaIfNeeded(EntryEventImpl event) {
    if (partitionedRegion.getSystem().getConfig().getDeltaPropagation()
        && event.getOperation().isUpdate() && event.getDeltaBytes() == null) {
      @Unretained
      Object rawNewValue = event.getRawNewValue();
      if (!(rawNewValue instanceof CachedDeserializable)) {
        return;
      }
      CachedDeserializable cd = (CachedDeserializable) rawNewValue;
      if (!cd.isSerialized()) {
        // it is a byte[]; not a Delta
        return;
      }
      Object instance = cd.getValue();
      if (instance instanceof org.apache.geode.Delta
          && ((org.apache.geode.Delta) instance).hasDelta()) {
        try {
          HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
          long start = DistributionStats.getStatTime();
          ((org.apache.geode.Delta) instance).toDelta(hdos);
          event.setDeltaBytes(hdos.toByteArray());
          partitionedRegion.getCachePerfStats().endDeltaPrepared(start);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new DeltaSerializationException(
              "Caught exception while sending delta. ",
              e);
        }
      }
    }
  }

  /**
   * create a PutAllPRMessage for notify-only and send it to all adjunct nodes. return a set of
   * members that should be attached to the operation's reply processor (if any)
   *
   * @param dpao DistributedPutAllOperation object for PutAllMessage
   * @param cacheOpRecipients set of receiver which got cacheUpdateOperation.
   * @param adjunctRecipients recipients that must unconditionally get the event
   * @param filterRoutingInfo routing information for all members having the region
   * @param processor the reply processor, or null if there isn't one
   */
  void performPutAllAdjunctMessaging(DistributedPutAllOperation dpao, Set cacheOpRecipients,
      Set<InternalDistributedMember> adjunctRecipients, FilterRoutingInfo filterRoutingInfo,
      DirectReplyProcessor processor) {
    PutAllPRMessage prMsg = dpao.createPRMessagesNotifyOnly(getId());
    prMsg.initMessage(partitionedRegion, adjunctRecipients, true, processor);
    prMsg.setSender(partitionedRegion.getDistributionManager().getDistributionManagerId());
    partitionedRegion.getDistributionManager().putOutgoing(prMsg);
  }

  /**
   * create a RemoveAllPRMessage for notify-only and send it to all adjunct nodes. return a set of
   * members that should be attached to the operation's reply processor (if any)
   *
   * @param op DistributedRemoveAllOperation object for RemoveAllMessage
   * @param cacheOpRecipients set of receiver which got cacheUpdateOperation.
   * @param adjunctRecipients recipients that must unconditionally get the event
   * @param filterRoutingInfo routing information for all members having the region
   * @param processor the reply processor, or null if there isn't one
   */
  void performRemoveAllAdjunctMessaging(DistributedRemoveAllOperation op,
      Set cacheOpRecipients, Set<InternalDistributedMember> adjunctRecipients,
      FilterRoutingInfo filterRoutingInfo,
      DirectReplyProcessor processor) {
    // create a RemoveAllPRMessage out of RemoveAllMessage to send to adjunct nodes
    RemoveAllPRMessage prMsg = op.createPRMessagesNotifyOnly(getId());
    prMsg.initMessage(partitionedRegion, adjunctRecipients, true, processor);
    prMsg.setSender(partitionedRegion.getDistributionManager().getDistributionManagerId());
    partitionedRegion.getDistributionManager().putOutgoing(prMsg);
  }

  /**
   * return the set of recipients for adjunct operations
   */
  protected Set<InternalDistributedMember> getAdjunctReceivers(EntryEventImpl event,
      Set<InternalDistributedMember> cacheOpReceivers, Set<InternalDistributedMember> twoMessages,
      FilterRoutingInfo routing) {
    Operation op = event.getOperation();
    if (op == null) {
      return Collections.emptySet();
    }
    if (op.isUpdate() || op.isCreate() || op.isDestroy() || op.isInvalidate()) {
      // this method can safely assume that the operation is being distributed from
      // the primary bucket holder to other nodes
      Set<InternalDistributedMember> r =
          partitionedRegion.getRegionAdvisor().adviseRequiresNotification();
      r.removeAll(cacheOpReceivers);

      // buckets that are initializing may transition out of token mode during
      // message transmission and need both cache-op and adjunct messages to
      // ensure that listeners are invoked
      if (twoMessages.size() > 0) {
        if (r.size() == 0) { // can't add to Collections.EMPTY_SET
          r = twoMessages;
        } else {
          r.addAll(twoMessages);
        }
      }
      if (routing != null) {
        // add adjunct messages to members with client routings
        for (InternalDistributedMember id : routing.getMembers()) {
          if (!cacheOpReceivers.contains(id)) {
            if (r.isEmpty()) {
              r = new HashSet<>();
            }
            r.add(id);
          }
        }
      }
      return r;
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public int getId() {
    return getBucketAdvisor().getProxyBucketRegion().getId();
  }

  @Override
  public void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients,
      CacheWriter localWriter, boolean requireOldValue, Object expectedOldValue)
      throws CacheWriterException, TimeoutException {

    boolean origRemoteState = false;
    try {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        origRemoteState = event.isOriginRemote();
        event.setOriginRemote(true);
      }
      event.setRegion(partitionedRegion);
      partitionedRegion.cacheWriteBeforePut(event, netWriteRecipients, localWriter,
          requireOldValue, expectedOldValue);
    } finally {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        event.setOriginRemote(origRemoteState);
      }
      event.setRegion(this);
    }
  }

  @Override
  public boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {

    boolean origRemoteState = false;
    try {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        origRemoteState = event.isOriginRemote();
        event.setOriginRemote(true);
      }
      event.setRegion(partitionedRegion);
      return partitionedRegion.cacheWriteBeforeDestroy(event, expectedOldValue);
    } finally {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        event.setOriginRemote(origRemoteState);
      }
      event.setRegion(this);
    }
  }

  @Override
  public CacheWriter basicGetWriter() {
    return partitionedRegion.basicGetWriter();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.partitioned.Bucket#getBucketOwners()
   *
   * @since GemFire 5.9
   */
  @Override
  public Set<InternalDistributedMember> getBucketOwners() {
    return getBucketAdvisor().getProxyBucketRegion().getBucketOwners();
  }

  public long getCounter() {
    return counter.get();
  }

  public void setCounter(AtomicLong counter) {
    this.counter = counter;
  }

  void updateCounter(long delta) {
    if (delta != 0) {
      counter.getAndAdd(delta);
    }
  }

  public void resetCounter() {
    if (counter.get() != 0) {
      counter.set(0);
    }
  }

  public long getLimit() {
    if (limit == null) {
      return 0;
    }
    return limit.get();
  }

  public void setLimit(long limit) {
    // This method can be called before object of this class is created
    if (this.limit == null) {
      this.limit = new AtomicLong();
    }
    this.limit.set(limit);
  }

  private static int calcMemSize(Object value) {
    if (value == null || value instanceof Token) {
      return 0;
    }
    if (!(value instanceof byte[]) && !(value instanceof CachedDeserializable)
        && !(value instanceof org.apache.geode.Delta)
        && !(value instanceof GatewaySenderEventImpl)) {
      // ezoerner:20090401 it's possible this value is a Delta
      throw new InternalGemFireError(
          "DEBUG: calcMemSize: weird value (class " + value.getClass() + "): " + value);
    }

    try {
      return CachedDeserializableFactory.calcMemSize(value);
    } catch (IllegalArgumentException e) {
      return 0;
    }
  }

  boolean isDestroyingDiskRegion;

  @Override
  void updateSizeOnClearRegion(int sizeBeforeClear) {
    // This method is only called when the bucket is destroyed. If we
    // start supporting clear of partitioned regions, this logic needs to change
    // we can't just set these counters to zero, because there could be
    // concurrent operations that are also updating these stats. For example,
    // a destroy could have already been applied to the map, and then updates
    // the stat after we reset it, making the state negative.

    final PartitionedRegionDataStore prDs = partitionedRegion.getDataStore();
    long oldMemValue;

    if (isDestroyed || isDestroyingDiskRegion) {
      // If this region is destroyed, mark the stat as destroyed.
      oldMemValue = bytesInMemory.getAndSet(BUCKET_DESTROYED);

    } else if (!isInitialized()) {
      // This case is rather special. We clear the region if the GII failed.
      // In the case of bucket regions, we know that there will be no concurrent operations
      // if GII has failed, because there is not primary. So it's safe to set these
      // counters to 0.
      oldMemValue = bytesInMemory.getAndSet(0);
    }

    else {
      throw new InternalGemFireError(
          "Trying to clear a bucket region that was not destroyed or in initialization.");
    }
    if (oldMemValue != BUCKET_DESTROYED) {
      partitionedRegion.getPrStats().incDataStoreEntryCount(-sizeBeforeClear);
      prDs.updateMemoryStats(-oldMemValue);
    }
  }

  @Override
  public int calculateValueSize(Object value) {
    // Only needed by BucketRegion
    return calcMemSize(value);
  }

  @Override
  public int calculateRegionEntryValueSize(RegionEntry regionEntry) {
    return calcMemSize(regionEntry.getValue()); // OFFHEAP _getValue ok
  }

  @Override
  public void updateSizeOnPut(Object key, int oldSize, int newSize) {
    updateBucket2Size(oldSize, newSize, SizeOp.UPDATE);
  }

  @Override
  public void updateSizeOnCreate(Object key, int newSize) {
    partitionedRegion.getPrStats().incDataStoreEntryCount(1);
    updateBucket2Size(0, newSize, SizeOp.CREATE);
  }

  @Override
  public void updateSizeOnRemove(Object key, int oldSize) {
    partitionedRegion.getPrStats().incDataStoreEntryCount(-1);
    updateBucket2Size(oldSize, 0, SizeOp.DESTROY);
  }

  @Override
  public int updateSizeOnEvict(Object key, int oldSize) {
    updateBucket2Size(oldSize, oldSize, SizeOp.EVICT);
    return oldSize;
  }

  @Override
  public void updateSizeOnFaultIn(Object key, int newMemSize, int oldDiskSize) {
    updateBucket2Size(oldDiskSize, newMemSize, SizeOp.FAULT_IN);
  }

  @Override
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    super.initializeStats(numEntriesInVM, numOverflowOnDisk, numOverflowBytesOnDisk);
    incNumEntriesInVM(numEntriesInVM);
    incNumOverflowOnDisk(numOverflowOnDisk);
    incNumOverflowBytesOnDisk(numOverflowBytesOnDisk);
  }

  @Override
  void setMemoryThresholdFlag(MemoryEvent event) {
    Assert.assertTrue(false);
    // Bucket regions are not registered with ResourceListener,
    // and should not get this event
  }

  @Override
  void initialCriticalMembers(boolean localHeapIsCritical,
      Set<InternalDistributedMember> criticalMembers) {
    // The owner Partitioned Region handles critical threshold events
  }

  @Override
  void closeCallbacksExceptListener() {
    // closeCacheCallback(getCacheLoader()); - fix bug 40228 - do NOT close loader
    closeCacheCallback(getCacheWriter());
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      evictionController.closeBucket(this);
    }
  }

  public long getTotalBytes() {
    long result = bytesInMemory.get();
    if (result == BUCKET_DESTROYED) {
      return 0;
    }
    result += getNumOverflowBytesOnDisk();
    return result;
  }

  public long getBytesInMemory() {
    long result = bytesInMemory.get();
    if (result == BUCKET_DESTROYED) {
      return 0;
    }

    return result;
  }


  void preDestroyBucket(int bucketId) {}

  @Override
  public void cleanupFailedInitialization() {
    preDestroyBucket(getId());
    super.cleanupFailedInitialization();
  }

  void invokePartitionListenerAfterBucketRemoved() {
    PartitionListener[] partitionListeners = getPartitionedRegion().getPartitionListeners();
    if (partitionListeners == null || partitionListeners.length == 0) {
      return;
    }
    for (PartitionListener listener : partitionListeners) {
      if (listener != null) {
        listener.afterBucketRemoved(getId(), keySet());
      }
    }
  }

  void invokePartitionListenerAfterBucketCreated() {
    PartitionListener[] partitionListeners = getPartitionedRegion().getPartitionListeners();
    if (partitionListeners == null || partitionListeners.length == 0) {
      return;
    }
    for (PartitionListener listener : partitionListeners) {
      if (listener != null) {
        listener.afterBucketCreated(getId(), keySet());
      }
    }
  }

  enum SizeOp {
    UPDATE, CREATE, DESTROY, EVICT, FAULT_IN;

    int computeMemoryDelta(int oldSize, int newSize) {
      switch (this) {
        case CREATE:
          return newSize;
        case DESTROY:
          return -oldSize;
        case UPDATE:
          return newSize - oldSize;
        case EVICT:
          return -oldSize;
        case FAULT_IN:
          return newSize;
        default:
          throw new AssertionError("unhandled sizeOp: " + this);
      }
    }
  }

  /**
   * Updates the bucket size.
   */
  private void updateBucket2Size(int oldSize, int newSize, SizeOp op) {

    final int memoryDelta = op.computeMemoryDelta(oldSize, newSize);

    if (memoryDelta == 0)
      return;
    // do the bigger one first to keep the sum > 0
    updateBucketMemoryStats(memoryDelta);
  }

  private void updateBucketMemoryStats(final int memoryDelta) {
    if (memoryDelta != 0) {

      final long bSize = bytesInMemory.compareAddAndGet(BUCKET_DESTROYED, memoryDelta);
      if (bSize == BUCKET_DESTROYED) {
        return;
      }

      if (bSize < 0 && !getCancelCriterion().isCancelInProgress()) {
        throw new InternalGemFireError("Bucket " + this + " size (" + bSize
            + ") negative after applying delta of " + memoryDelta);
      }
    }

    final PartitionedRegionDataStore prDS = partitionedRegion.getDataStore();
    prDS.updateMemoryStats(memoryDelta);
  }

  /**
   * Returns the current number of entries whose value has been overflowed to disk by this
   * bucket.This value will decrease when a value is faulted in.
   */
  public long getNumOverflowOnDisk() {
    return numOverflowOnDisk.get();
  }

  public long getNumOverflowBytesOnDisk() {
    return numOverflowBytesOnDisk.get();
  }

  /**
   * Returns the current number of entries whose value resides in the VM for this bucket. This value
   * will decrease when the entry is overflowed to disk.
   */
  public long getNumEntriesInVM() {
    return numEntriesInVM.get();
  }

  /**
   * Increments the current number of entries whose value has been overflowed to disk by this
   * bucket, by a given amount.
   */
  public void incNumOverflowOnDisk(long delta) {
    numOverflowOnDisk.addAndGet(delta);
  }

  public void incNumOverflowBytesOnDisk(long delta) {
    if (delta == 0)
      return;
    numOverflowBytesOnDisk.addAndGet(delta);
    // The following could be reenabled at a future time.
    // I deadcoded for now to make sure I didn't have it break
    // the last 6.5 regression.
    // It is possible that numOverflowBytesOnDisk might go negative
    // for a short period of time if a decrement ever happens before
    // its corresponding increment.
    // if (res < 0) {
    // throw new IllegalStateException("numOverflowBytesOnDisk < 0 " + res);
    // }
  }

  /**
   * Increments the current number of entries whose value has been overflowed to disk by this
   * bucket,by a given amount.
   */
  public void incNumEntriesInVM(long delta) {
    numEntriesInVM.addAndGet(delta);
  }

  @Override
  void incBucketEvictions() {
    evictions.getAndAdd(1);
  }

  public long getBucketEvictions() {
    return evictions.get();
  }

  @Override
  boolean isMemoryThresholdReachedForLoad() {
    return getBucketAdvisor().getProxyBucketRegion().isBucketSick();
  }

  public int getSizeForEviction() {
    EvictionAttributes ea = getAttributes().getEvictionAttributes();
    if (ea == null)
      return 0;
    EvictionAlgorithm algo = ea.getAlgorithm();
    if (!algo.isLRUHeap())
      return 0;
    EvictionAction action = ea.getAction();
    return action.isLocalDestroy() ? getRegionMap().sizeInVM() : (int) getNumEntriesInVM();
  }

  @Override
  HashMap getDestroyedSubregionSerialNumbers() {
    return new HashMap(0);
  }

  @Override
  public FilterProfile getFilterProfile() {
    return partitionedRegion.getFilterProfile();
  }

  @Override
  public void setCloningEnabled(boolean isCloningEnabled) {
    partitionedRegion.setCloningEnabled(isCloningEnabled);
  }

  @Override
  public boolean getCloningEnabled() {
    return partitionedRegion.getCloningEnabled();
  }

  @Override
  void generateLocalFilterRouting(InternalCacheEvent event) {
    if (event.getLocalFilterInfo() == null) {
      super.generateLocalFilterRouting(event);
    }
  }

  public void beforeAcquiringPrimaryState() {}

  public void afterAcquiringPrimaryState() {

  }

  /**
   * Invoked when a primary bucket is demoted.
   */
  void beforeReleasingPrimaryLockDuringDemotion() {}

  @Override
  public RegionAttributes getAttributes() {
    return this;
  }

  @Override
  public boolean notifiesSerialGatewaySender() {
    return getPartitionedRegion().notifiesSerialGatewaySender();
  }

  @Override
  public boolean hasSeenEvent(EntryEventImpl event) {
    ensureEventTrackerInitialization();
    return super.hasSeenEvent(event);
  }

  // bug 41289 - wait for event tracker to be initialized before checkin
  // so that an operation intended for a previous version of a bucket
  // is not prematurely applied to a new version of the bucket
  private void ensureEventTrackerInitialization() {
    try {
      getEventTracker().waitOnInitialization();
    } catch (InterruptedException ie) {
      getCancelCriterion().checkCancelInProgress(ie);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void postDestroyRegion(boolean destroyDiskRegion, RegionEventImpl event) {
    DiskRegion dr = getDiskRegion();
    if (dr != null && destroyDiskRegion) {
      dr.statsClear(this);
    }
    super.postDestroyRegion(destroyDiskRegion, event);
  }

  @Override
  public EvictionController getExistingController(InternalRegionArguments internalArgs) {
    return internalArgs.getPartitionedRegion().getEvictionController();
  }

  @Override
  public String getNameForStats() {
    return getPartitionedRegion().getFullPath();
  }

  @Override
  public void closeEntries() {
    entries.close(this);
  }

  @Override
  public Set<VersionSource> clearEntries(RegionVersionVector rvv) {
    return entries.clear(rvv, this);
  }

  @Override
  SenderIdMonitor createSenderIdMonitor() {
    // bucket regions do not need to monitor sender ids
    return null;
  }

  @Override
  void updateSenderIdMonitor() {
    // nothing needed on a bucket region
  }

  @Override
  void checkSameSenderIdsAvailableOnAllNodes() {
    // nothing needed on a bucket region
  }
}
