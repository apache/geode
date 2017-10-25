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

import org.apache.geode.*;
import org.apache.geode.cache.*;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.AtomicLongWithTerminalState;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.CreateRegionProcessor.CreateRegionReplyProcessor;
import org.apache.geode.internal.cache.event.EventSequenceNumberHolder;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.control.MemoryEvent;
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
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.concurrent.AtomicLong5;
import org.apache.geode.internal.concurrent.Atomics;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;


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

  public static final RawValue NULLVALUE = new RawValue(null);
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
      this.rawValue = rawVal;
    }

    public boolean isValueByteArray() {
      return this.rawValue instanceof byte[];
    }

    public Object getRawValue() {
      return this.rawValue;
    }

    public void writeAsByteArray(DataOutput out) throws IOException {
      if (isValueByteArray()) {
        DataSerializer.writeByteArray((byte[]) this.rawValue, out);
      } else if (this.rawValue instanceof CachedDeserializable) {
        ((CachedDeserializable) this.rawValue).writeValueAsByteArray(out);
      } else if (Token.isInvalid(this.rawValue)) {
        DataSerializer.writeByteArray(null, out);
      } else if (this.rawValue == Token.TOMBSTONE) {
        DataSerializer.writeByteArray(null, out);
      } else {
        DataSerializer.writeObjectAsByteArray(this.rawValue, out);
      }
    }

    @Override
    public String toString() {
      return "RawValue(" + this.rawValue + ")";
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
          byte[] src = (byte[]) this.rawValue;
          byte[] dest = new byte[src.length];
          System.arraycopy(this.rawValue, 0, dest, 0, dest.length);
          return dest;
        } else {
          return this.rawValue;
        }
      } else if (this.rawValue instanceof CachedDeserializable) {
        if (copyOnRead) {
          return ((CachedDeserializable) this.rawValue).getDeserializedWritableCopy(null, null);
        } else {
          return ((CachedDeserializable) this.rawValue).getDeserializedForReading();
        }
      } else if (Token.isInvalid(this.rawValue)) {
        return null;
      } else {
        if (copyOnRead) {
          return CopyHelper.copy(this.rawValue);
        } else {
          return this.rawValue;
        }
      }
    }
  }

  private static final long serialVersionUID = 1L;

  private final int redundancy;

  /** the partitioned region to which this bucket belongs */
  private final PartitionedRegion partitionedRegion;
  private final Map<Object, ExpiryTask> pendingSecondaryExpires = new HashMap<Object, ExpiryTask>();

  /* one map per bucket region */
  public HashMap allKeysMap = new HashMap();

  static final boolean FORCE_LOCAL_LISTENERS_INVOCATION = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "BucketRegion.alwaysFireLocalListeners");

  private volatile AtomicLong5 eventSeqNum = null;

  public AtomicLong5 getEventSeqNum() {
    return eventSeqNum;
  }

  public BucketRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs) {
    super(regionName, attrs, parentRegion, cache, internalRegionArgs);
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
    this.redundancy = internalRegionArgs.getPartitionedRegionBucketRedundancy();
    this.partitionedRegion = internalRegionArgs.getPartitionedRegion();
  }

  // Attempt to direct the GII process to the primary first
  @Override
  protected void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException {
    // Set this region in the ProxyBucketRegion early so that profile exchange will
    // perform the correct fillInProfile method
    getBucketAdvisor().getProxyBucketRegion().setBucketRegion(this);
    boolean success = false;
    try {
      if (this.partitionedRegion.isShadowPR()
          && this.partitionedRegion.getColocatedWith() != null) {
        PartitionedRegion parentPR = ColocationHelper.getLeaderRegion(this.partitionedRegion);
        BucketRegion parentBucket = parentPR.getDataStore().getLocalBucketById(getId());
        // needs to be set only once.
        if (parentBucket.eventSeqNum == null) {
          parentBucket.eventSeqNum = new AtomicLong5(getId());
        }
      }
      if (this.partitionedRegion.getColocatedWith() == null) {
        this.eventSeqNum = new AtomicLong5(getId());
      } else {
        PartitionedRegion parentPR = ColocationHelper.getLeaderRegion(this.partitionedRegion);
        BucketRegion parentBucket = parentPR.getDataStore().getLocalBucketById(getId());
        if (parentBucket == null && logger.isDebugEnabled()) {
          logger.debug("The parentBucket of region {} bucketId {} is NULL",
              this.partitionedRegion.getFullPath(), getId());
        }
        Assert.assertTrue(parentBucket != null);
        this.eventSeqNum = parentBucket.eventSeqNum;
      }

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



  @Override
  public void initialized() {
    // announce that the bucket is ready
    // setHosting performs a profile exchange, so there
    // is no need to call super.initialized() here.
  }

  @Override
  protected DiskStoreImpl findDiskStore(RegionAttributes regionAttributes,
      InternalRegionArguments internalRegionArgs) {
    return internalRegionArgs.getPartitionedRegion().getDiskStore();
  }

  @Override
  public void registerCreateRegionReplyProcessor(CreateRegionReplyProcessor processor) {
    this.createRegionReplyProcessor = processor;
  }

  @Override
  protected void recordEventStateFromImageProvider(InternalDistributedMember provider) {
    if (this.createRegionReplyProcessor != null) {
      Map<ThreadIdentifier, EventSequenceNumberHolder> providerEventStates =
          this.createRegionReplyProcessor.getEventState(provider);
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
      this.createRegionReplyProcessor = null;
    }
  }

  @Override
  protected CacheDistributionAdvisor createDistributionAdvisor(
      InternalRegionArguments internalRegionArgs) {
    return internalRegionArgs.getBucketAdvisor();
  }

  public BucketAdvisor getBucketAdvisor() {
    return (BucketAdvisor) getDistributionAdvisor();
  }

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
  protected boolean needsTombstoneGCKeysForClients(EventID eventID, FilterInfo clientRouting) {
    if (eventID == null) {
      return false;
    }
    if (CacheClientNotifier.getInstance() == null) {
      return false;
    }
    if (clientRouting != null) {
      return true;
    }
    if (getFilterProfile() != null) {
      return true;
    }
    return false;
  }

  @Override
  protected void notifyClientsOfTombstoneGC(Map<VersionSource, Long> regionGCVersions,
      Set<Object> removedKeys, EventID eventID, FilterInfo routing) {
    if (CacheClientNotifier.getInstance() != null) {
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
  private LockObject searchAndLock(Object keys[]) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    LockObject foundLock = null;

    synchronized (allKeysMap) {
      // check if there's any key in map
      for (int i = 0; i < keys.length; i++) {
        if (allKeysMap.containsKey(keys[i])) {
          foundLock = (LockObject) allKeysMap.get(keys[i]);
          if (isDebugEnabled) {
            logger.debug("LockKeys: found key: {}:{}", keys[i], foundLock.lockedTimeStamp);
          }
          foundLock.waiting();
          break;
        }
      }

      // save the keys when still locked
      if (foundLock == null) {
        for (int i = 0; i < keys.length; i++) {
          LockObject lockValue =
              new LockObject(keys[i], isDebugEnabled ? System.currentTimeMillis() : 0);
          allKeysMap.put(keys[i], lockValue);
          if (isDebugEnabled) {
            logger.debug("LockKeys: add key: {}:{}", keys[i], lockValue.lockedTimeStamp);
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
  public void removeAndNotifyKeys(Object keys[]) {
    final boolean isTraceEnabled = logger.isTraceEnabled();

    synchronized (allKeysMap) {
      for (int i = 0; i < keys.length; i++) {
        LockObject lockValue = (LockObject) allKeysMap.remove(keys[i]);
        if (lockValue != null) {
          // let current thread become the monitor of the key object
          synchronized (lockValue) {
            lockValue.setRemoved();
            if (isTraceEnabled) {
              long waitTime = System.currentTimeMillis() - lockValue.lockedTimeStamp;
              logger.trace("LockKeys: remove key {}, notifyAll for {}. It waited {}", keys[i],
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
  public void waitUntilLocked(Object keys[]) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final String title = "BucketRegion.waitUntilLocked:";
    while (true) {
      LockObject foundLock = searchAndLock(keys);

      if (foundLock != null) {
        synchronized (foundLock) {
          try {
            while (!foundLock.isRemoved()) {
              this.partitionedRegion.checkReadiness();
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
        break;
      } // to lock and process
    } // while
  }

  // Entry (Put/Create) rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka update or create entry
  // 2) distribute op to bucket secondaries and bridge servers with synchrony on local entry
  // 3) cache listener with synchrony on entry
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  protected boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws TimeoutException, CacheWriterException {
    beginLocalWrite(event);

    try {
      if (this.partitionedRegion.isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      if (!hasSeenEvent(event)) {
        forceSerialized(event);
        RegionEntry oldEntry = this.entries.basicPut(event, lastModified, ifNew, ifOld,
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
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "BR.virtualPut: this cache has already seen this event {}",
            event);
      }
      if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
        distributeUpdateOperation(event, lastModified);
      }
      return true;
    } finally {
      endLocalWrite(event);
    }
  }


  public long generateTailKey() {
    long key = this.eventSeqNum.addAndGet(this.partitionedRegion.getTotalNumberOfBuckets());
    if (key < 0 || key % getPartitionedRegion().getTotalNumberOfBuckets() != getId()) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.GatewaySender_SEQUENCENUMBER_GENERATED_FOR_EVENT_IS_INVALID,
          new Object[] {key, getId()}));
    }
    if (logger.isDebugEnabled()) {
      logger.debug("WAN: On primary bucket {}, setting the seq number as {}", getId(),
          this.eventSeqNum.get());
    }
    return eventSeqNum.get();
  }

  public void handleWANEvent(EntryEventImpl event) {
    if (this.eventSeqNum == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The bucket corresponding to this user bucket is not created yet. This event will not go to remote wan site. Event: {}",
            event);
      }
    }

    if (!(this instanceof AbstractBucketRegionQueue)) {
      if (getBucketAdvisor().isPrimary()) {
        long key = this.eventSeqNum.addAndGet(this.partitionedRegion.getTotalNumberOfBuckets());
        if (key < 0 || key % getPartitionedRegion().getTotalNumberOfBuckets() != getId()) {
          logger.error(LocalizedMessage.create(
              LocalizedStrings.GatewaySender_SEQUENCENUMBER_GENERATED_FOR_EVENT_IS_INVALID,
              new Object[] {key, getId()}));
        }
        event.setTailKey(key);
        if (logger.isDebugEnabled()) {
          logger.debug("WAN: On primary bucket {}, setting the seq number as {}", getId(),
              this.eventSeqNum.get());
        }
      } else {
        // Can there be a race here? Like one thread has done put in primary but
        // its update comes later
        // in that case its possible that a tail key is missed.
        // we can handle that by only incrementing the tailKey and never
        // setting it less than the current value.
        Atomics.setIfGreater(this.eventSeqNum, event.getTailKey());
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
  public void updateEventSeqNum(long l) {
    Atomics.setIfGreater(this.eventSeqNum, l);
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
          event.getPutAllOperation().addEntry(event, this.getId());
        } else {
          // before distribute: BR's put
          op = new UpdateOperation(event, lastModified);
          token = op.startOperation();
          if (logger.isDebugEnabled()) {
            logger.debug("sent update operation : for region  : {}: with event: {}", this.getName(),
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
  protected long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
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
      if (this.partitionedRegion.getDataStore().hasClientInterest(event)) {
        updateStatsForGet(entry, true);
      }
      if (!event.isOriginRemote()) {
        if (event.getVersionTag() == null || event.getVersionTag().isGatewayTag()) {
          boolean eventHasDelta = event.getDeltaBytes() != null;
          VersionTag v = entry.generateVersionTag(null, eventHasDelta, this, event);
          if (v != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("generated version tag {} in region {}", v, this.getName());
            }
          }
        }

        // This code assumes it is safe ignore token mode (GII in progress)
        // because it assumes when the origin of the event is local,
        // the GII has completed and the region is initialized and open for local
        // ops

        if (!event.isBulkOpInProgress()) {
          long start = this.partitionedRegion.getPrStats().startSendReplication();
          try {
            // before distribute: PR's put PR
            op = new UpdateOperation(event, modifiedTime);
            token = op.startOperation();
          } finally {
            this.partitionedRegion.getPrStats().endSendReplication(start);
          }
        } else {
          // consolidate the UpdateOperation for each entry into a PutAllMessage
          // basicPutPart3 takes care of this
        }
      }

      long lastModifiedTime =
          super.basicPutPart2(event, entry, isInitialized, lastModified, clearConflict);
      return lastModifiedTime;
    } finally {
      if (op != null) {
        op.endOperation(token);
      }
    }
  }

  protected void notifyGatewaySender(EnumListenerEvent operation, EntryEventImpl event) {
    // We don't need to clone the event for new Gateway Senders.
    // Preserve the bucket reference for resetting it later.
    LocalRegion bucketRegion = event.getRegion();
    try {
      event.setRegion(this.partitionedRegion);
      this.partitionedRegion.notifyGatewaySender(operation, event);
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
      this.partitionedRegion.checkReadiness();
      checkReadiness();
      InternalDistributedMember primaryHolder = getBucketAdvisor().basicGetPrimaryMember();
      throw new PrimaryBucketException(
          "Bucket " + getName() + " is not primary. Current primary holder is " + primaryHolder);
    }
  }

  /**
   * Checks to make sure that this node is primary, and locks the bucket to make sure the bucket
   * stays the primary bucket while the write is in progress. Any call to this method must be
   * followed with a call to endLocalWrite().
   */
  private boolean beginLocalWrite(EntryEventImpl event) {
    if (!needWriteLock(event)) {
      return false;
    }

    if (cache.isCacheAtShutdownAll()) {
      throw new CacheClosedException("Cache is shutting down");
    }

    Object keys[] = new Object[1];
    keys[0] = event.getKey();
    waitUntilLocked(keys); // it might wait for long time

    boolean lockedForPrimary = false;
    try {
      doLockForPrimary(false);
      return lockedForPrimary = true;
    } finally {
      if (!lockedForPrimary) {
        removeAndNotifyKeys(keys);
      }
    }
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
        throw new CacheClosedException("Cache is shutting down");
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
    Lock activeWriteLock = this.getBucketAdvisor().getActiveWriteLock();
    Lock parentLock = this.getBucketAdvisor().getParentActiveWriteLock();
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
            boolean locked = activeWriteLock.tryLock();
            if (!locked) {
              parentLock.unlock();
              return false;
            }
          } else {
            activeWriteLock.lockInterruptibly();
          }
        } else {
          if (tryLock) {
            boolean locked = activeWriteLock.tryLock();
            if (!locked) {
              return false;
            }
          } else {
            activeWriteLock.lockInterruptibly();
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
    Lock activeWriteLock = this.getBucketAdvisor().getActiveWriteLock();
    activeWriteLock.unlock();
    Lock parentLock = this.getBucketAdvisor().getParentActiveWriteLock();
    if (parentLock != null) {
      parentLock.unlock();
    }
  }

  /**
   * Release the lock on the bucket that makes the bucket stay the primary during a write.
   */
  private void endLocalWrite(EntryEventImpl event) {
    if (!needWriteLock(event)) {
      return;
    }


    doUnlockForPrimary();

    Object keys[] = new Object[1];
    keys[0] = event.getKey();
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
  // 2) distribute op to bucket secondaries and bridge servers with synchrony on local entry
  // 3) cache listener with synchrony on entry
  // 4) update local bs, gateway
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  void basicInvalidate(EntryEventImpl event) throws EntryNotFoundException {
    basicInvalidate(event, isInitialized(), false);
  }

  @Override
  void basicInvalidate(final EntryEventImpl event, boolean invokeCallbacks, boolean forceNewEntry)
      throws EntryNotFoundException {
    // disallow local invalidation
    Assert.assertTrue(!event.isLocalInvalid());
    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    beginLocalWrite(event);
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
            this.entries.invalidate(event, invokeCallbacks, forceNewEntry, forceCallbacks);
        ExpirationAction expirationAction = getEntryExpirationAction();
        if (done && !getBucketAdvisor().isPrimary() && expirationAction != null
            && expirationAction.isInvalidate()) {
          synchronized (pendingSecondaryExpires) {
            pendingSecondaryExpires.remove(event.getKey());
          }
        }
        return;
      } else {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM,
              "LR.basicInvalidate: this cache has already seen this event {}", event);
        }
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeInvalidateOperation(event);
        }
        return;
      }
    } finally {
      endLocalWrite(event);
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
            logger.debug("generated version tag {} in region {}", v, this.getName());
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
    event.region = this.partitionedRegion;
  }

  @Override
  protected boolean shouldDistributeInvalidateRegion(RegionEventImpl event) {
    return getBucketAdvisor().isPrimary();
  }

  @Override
  protected boolean shouldGenerateVersionTag(RegionEntry entry, EntryEventImpl event) {
    if (event.getOperation().isLocal()) { // bug #45402 - localDestroy generated a version tag
      return false;
    }
    return this.concurrencyChecksEnabled
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
      return;
    } catch (PrimaryBucketException e) {
      // must have concurrently removed the primary
      return;
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
    ExpiryTask task = expiryTask;
    boolean isEvictDestroy = isEntryEvictDestroyEnabled();
    // Fix for bug 43805 - get the primary lock before
    // synchronizing on pendingSecondaryExpires, to match the lock
    // ordering in other place (like acquiredPrimaryLock)
    lockPrimaryStateReadLock(false);
    try {
      // Why do we care if evict destroy is configured?
      // See bug 41096 for the answer.
      if (!getBucketAdvisor().isPrimary() && !isEvictDestroy) {
        synchronized (this.pendingSecondaryExpires) {
          if (task.isPending()) {
            Object key = task.getKey();
            if (key != null) {
              this.pendingSecondaryExpires.put(key, task);
            }
          }
        }
      } else {
        super.performExpiryTimeout(task);
      }
    } finally {
      doUnlockForPrimary();
    }
  }

  protected boolean isEntryEvictDestroyEnabled() {
    return getEvictionAttributes() != null
        && EvictionAction.LOCAL_DESTROY.equals(getEvictionAttributes().getAction());
  }

  protected void processPendingSecondaryExpires() {
    ExpiryTask[] tasks;
    while (true) {
      // note we just keep looping until no more pendingExpires exist
      synchronized (this.pendingSecondaryExpires) {
        if (this.pendingSecondaryExpires.isEmpty()) {
          return;
        }
        tasks = new ExpiryTask[this.pendingSecondaryExpires.size()];
        tasks = this.pendingSecondaryExpires.values().toArray(tasks);
        this.pendingSecondaryExpires.clear();
      }
      try {
        if (isCacheClosing() || isClosed() || this.isDestroyed) {
          return;
        }
        final boolean isDebugEnabled = logger.isDebugEnabled();
        for (int i = 0; i < tasks.length; i++) {
          try {
            if (isDebugEnabled) {
              logger.debug("{} fired at {}", tasks[i], System.currentTimeMillis());
            }
            tasks[i].basicPerformTimeout(true);
            if (isCacheClosing() || isClosed() || isDestroyed()) {
              return;
            }
          } catch (EntryNotFoundException ignore) {
            // ignore and try the next expiry task
          }
        }
      } catch (RegionDestroyedException re) {
        // Ignore - our job is done
      } catch (CancelException ex) {
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
        logger.fatal(
            LocalizedMessage.create(LocalizedStrings.LocalRegion_EXCEPTION_IN_EXPIRATION_TASK), ex);
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
  protected EntryEventImpl generateEvictDestroyEvent(Object key) {
    EntryEventImpl event = super.generateEvictDestroyEvent(key);
    event.setInvokePRCallbacks(true); // see bug 40797
    return event;
  }

  // Entry Destruction rules
  // If this is a primary for the bucket
  // 1) apply op locally, aka destroy entry (REMOVED token)
  // 2) distribute op to bucket secondaries and bridge servers with synchrony on local entry
  // 3) cache listener with synchrony on local entry
  // 4) update local bs, gateway
  // Else not a primary
  // 1) apply op locally
  // 2) update local bs, gateway
  @Override
  protected void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
      Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {

    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    beginLocalWrite(event);
    try {
      // increment the tailKey for the destroy event
      if (this.partitionedRegion.isParallelWanEnabled()) {
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
        return;
      } else {
        if (!getConcurrencyChecksEnabled() || event.hasValidVersionTag()) {
          distributeDestroyOperation(event);
        }
        return;
      }
    } finally {
      endLocalWrite(event);
    }
  }

  protected void distributeDestroyOperation(EntryEventImpl event) {
    long token = -1;
    DestroyOperation op = null;

    try {
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "BR.basicDestroy: this cache has already seen this event {}",
            event);
      }
      if (!event.isOriginRemote() && getBucketAdvisor().isPrimary()) {
        if (event.isBulkOpInProgress()) {
          // consolidate the DestroyOperation for each entry into a RemoveAllMessage
          event.getRemoveAllOperation().addEntry(event, this.getId());
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
  protected void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event) {
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
            logger.debug("generated version tag {} in region {}", v, this.getName());
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
  protected void validateArguments(Object key, Object value, Object aCallbackArgument) {
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
  protected RegionEntry basicPutEntry(final EntryEventImpl event, final long lastModified)
      throws TimeoutException, CacheWriterException {
    beginLocalWrite(event);
    try {
      if (getPartitionedRegion().isParallelWanEnabled()) {
        handleWANEvent(event);
      }
      event.setInvokePRCallbacks(true);
      forceSerialized(event);
      return super.basicPutEntry(event, lastModified);
    } finally {
      endLocalWrite(event);
    }
  }

  @Override
  void basicUpdateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {

    Assert.assertTrue(!isTX());
    Assert.assertTrue(event.getOperation().isDistributed());

    LocalRegion lr = event.getLocalRegion();
    AbstractRegionMap arm = ((AbstractRegionMap) lr.getRegionMap());
    try {
      arm.lockForCacheModification(lr, event);
      beginLocalWrite(event);
      try {
        if (!hasSeenEvent(event)) {
          this.entries.updateEntryVersion(event);
        } else {
          if (logger.isTraceEnabled(LogMarker.DM)) {
            logger.trace(LogMarker.DM,
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
        return;
      } finally {
        endLocalWrite(event);
      }
    } finally {
      arm.releaseCacheModificationLock(event.getLocalRegion(), event);
    }
  }

  protected void distributeUpdateEntryVersionOperation(EntryEventImpl event) {
    new UpdateEntryVersionOperation(event).distribute();
  }

  public int getRedundancyLevel() {
    return this.redundancy;
  }

  public boolean isPrimary() {
    throw new UnsupportedOperationException(
        LocalizedStrings.BucketRegion_THIS_SHOULD_NEVER_BE_CALLED_ON_0
            .toLocalizedString(getClass()));
  }

  @Override
  public boolean isDestroyed() {
    // TODO prpersist - Added this if null check for the partitioned region
    // because we create the disk store for a bucket *before* in the constructor
    // for local region, which is before this final field is assigned. This is why
    // we shouldn't do some much work in the constructors! This is a temporary
    // hack until I move must of the constructor code to region.initialize.
    return isBucketDestroyed() || (this.partitionedRegion != null
        && this.partitionedRegion.isLocallyDestroyed && !isInDestroyingThread());
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
  public int sizeEstimate() {
    return size();
  }

  @Override
  public int getRegionSize(DistributedMember target) {
    // GEODE-3679. Do not forward the request again.
    return getRegionSize();
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
    return this.partitionedRegion;
  }

  /**
   * is the current thread involved in destroying the PR that owns this region?
   */
  private boolean isInDestroyingThread() {
    return this.partitionedRegion.locallyDestroyingThread == Thread.currentThread();
  }

  @Override
  public void fillInProfile(Profile profile) {
    super.fillInProfile(profile);
    BucketProfile bp = (BucketProfile) profile;
    bp.isInitializing = this.initializationLatchAfterGetInitialImage.getCount() > 0;
  }

  /** check to see if the partitioned region is locally destroyed or closed */
  public boolean isPartitionedRegionOpen() {
    return !this.partitionedRegion.isLocallyDestroyed && !this.partitionedRegion.isClosed
        && !this.partitionedRegion.isDestroyed();
  }

  /**
   * Horribly plagiarized from the similar method in LocalRegion
   * 
   * @param clientEvent holder for client version tag
   * @param returnTombstones whether Token.TOMBSTONE should be returned for destroyed entries
   * @return serialized form if present, null if the entry is not in the cache, or INVALID or
   *         LOCAL_INVALID re is a miss (invalid)
   * @throws IOException if there is a serialization problem see
   *         LocalRegion#getDeserializedValue(RegionEntry, KeyInfo, boolean, boolean, boolean,
   *         EntryEventImpl, boolean, boolean, boolean)
   */
  private RawValue getSerialized(Object key, boolean updateStats, boolean doNotLockEntry,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws EntryNotFoundException, IOException {
    RegionEntry re = null;
    re = this.entries.getEntry(key);
    if (re == null) {
      return NULLVALUE;
    }
    if (re.isTombstone() && !returnTombstones) {
      return NULLVALUE;
    }
    Object v = null;

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
      this.handleDiskAccessException(dae);
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
   * @param returnTombstones TODO
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
      RawValue valueBytes = NULLVALUE;
      boolean isCreate = false;
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
    return new StringBuilder().append("BucketRegion").append("[path='").append(getFullPath())
        .append(";serial=").append(getSerialNumber()).append(";primary=")
        .append(getBucketAdvisor().getProxyBucketRegion().isPrimary()).append("]").toString();
  }

  @Override
  protected void distributedRegionCleanup(RegionEventImpl event) {
    // No need to close advisor, assume its already closed
    // However we need to remove our listener from the advisor (see bug 43950).
    this.distAdvisor.removeMembershipListener(this.advisorListener);
  }

  /**
   * Tell the peers that this VM has destroyed the region.
   * 
   * Also marks the local disk files as to be deleted before sending the message to peers.
   * 
   * 
   * @param rebalance true if this is due to a rebalance removing the bucket
   */
  public void removeFromPeersAdvisors(boolean rebalance) {
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
  protected void distributeDestroyRegion(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    // No need to do this when we actually destroy the region,
    // we already distributed this info.
  }

  @Retained
  EntryEventImpl createEventForPR(EntryEventImpl sourceEvent) {
    EntryEventImpl e2 = new EntryEventImpl(sourceEvent);
    boolean returned = false;
    try {
      e2.setRegion(this.partitionedRegion);
      if (FORCE_LOCAL_LISTENERS_INVOCATION) {
        e2.setInvokePRCallbacks(true);
      } else {
        e2.setInvokePRCallbacks(sourceEvent.getInvokePRCallbacks());
      }
      DistributedMember dm = this.getDistributionManager().getDistributionManagerId();
      e2.setOriginRemote(!e2.getDistributedMember().equals(dm));
      returned = true;
      return e2;
    } finally {
      if (!returned) {
        e2.release();
      }
    }
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
    if (this.isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeTXCallbacks(eventType, event, callThem);
    }
    @Released
    final EntryEventImpl prevent = createEventForPR(event);
    try {
      this.partitionedRegion.invokeTXCallbacks(eventType, prevent,
          this.partitionedRegion.isInitialized() ? callDispatchListenerEvent : false);
    } finally {
      prevent.release();
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
    if (this.isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && this.getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeDestroyCallbacks(eventType, event, callThem, notifyGateways);
    }
    @Released
    final EntryEventImpl prevent = createEventForPR(event);
    try {
      this.partitionedRegion.invokeDestroyCallbacks(eventType, prevent,
          this.partitionedRegion.isInitialized() ? callDispatchListenerEvent : false, false);
    } finally {
      prevent.release();
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
    if (this.isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (event.isPossibleDuplicate()
          && this.getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokeInvalidateCallbacks(eventType, event, callThem);
    }
    @Released
    final EntryEventImpl prevent = createEventForPR(event);
    try {
      this.partitionedRegion.invokeInvalidateCallbacks(eventType, prevent,
          this.partitionedRegion.isInitialized() ? callDispatchListenerEvent : false);
    } finally {
      prevent.release();
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
    if (this.isInitialized()) {
      boolean callThem = callDispatchListenerEvent;
      if (callThem && event.isPossibleDuplicate()
          && this.getEventTracker().isInitialImageProvider(event.getDistributedMember())) {
        callThem = false;
      }
      super.invokePutCallbacks(eventType, event, callThem, notifyGateways);
    }

    @Released
    final EntryEventImpl prevent = createEventForPR(event);
    try {
      this.partitionedRegion.invokePutCallbacks(eventType, prevent,
          this.partitionedRegion.isInitialized() ? callDispatchListenerEvent : false, false);
    } finally {
      prevent.release();
    }
  }

  /**
   * perform adjunct messaging for the given operation and return a set of members that should be
   * attached to the operation's reply processor (if any)
   * 
   * @param event the event causing this messaging
   * @param cacheOpRecipients set of receiver which got cacheUpdateOperation.
   * @param adjunctRecipients recipients that must unconditionally get the event
   * @param filterRoutingInfo routing information for all members having the region
   * @param processor the reply processor, or null if there isn't one
   * @return the set of failed recipients
   */
  protected Set performAdjunctMessaging(EntryEventImpl event, Set cacheOpRecipients,
      Set adjunctRecipients, FilterRoutingInfo filterRoutingInfo, DirectReplyProcessor processor,
      boolean calculateDelta, boolean sendDeltaWithFullValue) {

    Set failures = Collections.emptySet();
    PartitionMessage msg = event.getPartitionMessage();
    if (calculateDelta) {
      setDeltaIfNeeded(event);
    }
    if (msg != null) {
      // The primary bucket member which is being modified remotely by a
      // thread via a received PartitionedMessage
      msg = msg.getMessageForRelayToListeners(event, adjunctRecipients);
      msg.setSender(this.partitionedRegion.getDistributionManager().getDistributionManagerId());
      msg.setSendDeltaWithFullValue(sendDeltaWithFullValue);

      failures = msg.relayToListeners(cacheOpRecipients, adjunctRecipients, filterRoutingInfo,
          event, this.partitionedRegion, processor);
    } else {
      // The primary bucket is being modified locally by an application thread locally
      Operation op = event.getOperation();
      if (op.isCreate() || op.isUpdate()) {
        // note that at this point ifNew/ifOld have been used to update the
        // local store, and the event operation should be correct
        failures = PutMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, this.partitionedRegion, event, op.isCreate(), !op.isCreate(),
            processor, sendDeltaWithFullValue);
      } else if (op.isDestroy()) {
        failures = DestroyMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, this.partitionedRegion, event, processor);
      } else if (op.isInvalidate()) {
        failures = InvalidateMessage.notifyListeners(cacheOpRecipients, adjunctRecipients,
            filterRoutingInfo, this.partitionedRegion, event, processor);
      } else {
        failures = adjunctRecipients;
      }
    }
    return failures;
  }

  private void setDeltaIfNeeded(EntryEventImpl event) {
    if (this.partitionedRegion.getSystem().getConfig().getDeltaPropagation()
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
          this.partitionedRegion.getCachePerfStats().endDeltaPrepared(start);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new DeltaSerializationException(
              LocalizedStrings.DistributionManager_CAUGHT_EXCEPTION_WHILE_SENDING_DELTA
                  .toLocalizedString(),
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
   * @return the set of failed recipients
   */
  public Set performPutAllAdjunctMessaging(DistributedPutAllOperation dpao, Set cacheOpRecipients,
      Set adjunctRecipients, FilterRoutingInfo filterRoutingInfo, DirectReplyProcessor processor) {
    // create a PutAllPRMessage out of PutAllMessage to send to adjunct nodes
    PutAllPRMessage prMsg = dpao.createPRMessagesNotifyOnly(getId());
    prMsg.initMessage(this.partitionedRegion, adjunctRecipients, true, processor);
    prMsg.setSender(this.partitionedRegion.getDistributionManager().getDistributionManagerId());

    // find members who have clients subscribed to this event and add them
    // to the recipients list. Also determine if there are any FilterInfo
    // routing tables for any of the receivers
    // boolean anyWithRouting = false;
    Set recipients = null;
    Set membersWithRouting = filterRoutingInfo.getMembers();
    for (Iterator it = membersWithRouting.iterator(); it.hasNext();) {
      Object mbr = it.next();
      if (!cacheOpRecipients.contains(mbr)) {
        // anyWithRouting = true;
        if (!adjunctRecipients.contains(mbr)) {
          if (recipients == null) {
            recipients = new HashSet();
            recipients.add(mbr);
          }
        }
      }
    }
    if (recipients == null) {
      recipients = adjunctRecipients;
    } else {
      recipients.addAll(adjunctRecipients);
    }

    // Set failures = Collections.EMPTY_SET;

    // if (!anyWithRouting) {
    Set failures = this.partitionedRegion.getDistributionManager().putOutgoing(prMsg);

    return failures;
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
   * @return the set of failed recipients
   */
  public Set performRemoveAllAdjunctMessaging(DistributedRemoveAllOperation op,
      Set cacheOpRecipients, Set adjunctRecipients, FilterRoutingInfo filterRoutingInfo,
      DirectReplyProcessor processor) {
    // create a RemoveAllPRMessage out of RemoveAllMessage to send to adjunct nodes
    RemoveAllPRMessage prMsg = op.createPRMessagesNotifyOnly(getId());
    prMsg.initMessage(this.partitionedRegion, adjunctRecipients, true, processor);
    prMsg.setSender(this.partitionedRegion.getDistributionManager().getDistributionManagerId());

    // find members who have clients subscribed to this event and add them
    // to the recipients list. Also determine if there are any FilterInfo
    // routing tables for any of the receivers
    Set recipients = null;
    Set membersWithRouting = filterRoutingInfo.getMembers();
    for (Iterator it = membersWithRouting.iterator(); it.hasNext();) {
      Object mbr = it.next();
      if (!cacheOpRecipients.contains(mbr)) {
        // anyWithRouting = true;
        if (!adjunctRecipients.contains(mbr)) {
          if (recipients == null) {
            recipients = new HashSet();
            recipients.add(mbr);
          }
        }
      }
    }
    if (recipients == null) {
      recipients = adjunctRecipients;
    } else {
      recipients.addAll(adjunctRecipients);
    }

    Set failures = this.partitionedRegion.getDistributionManager().putOutgoing(prMsg);
    return failures;
  }

  /**
   * return the set of recipients for adjunct operations
   */
  protected Set getAdjunctReceivers(EntryEventImpl event, Set cacheOpReceivers, Set twoMessages,
      FilterRoutingInfo routing) {
    Operation op = event.getOperation();
    if (op.isUpdate() || op.isCreate() || op.isDestroy() || op.isInvalidate()) {
      // this method can safely assume that the operation is being distributed from
      // the primary bucket holder to other nodes
      Set r = this.partitionedRegion.getRegionAdvisor().adviseRequiresNotification(event);

      if (r.size() > 0) {
        r.removeAll(cacheOpReceivers);
      }

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
              r = new HashSet();
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

  public int getId() {
    return getBucketAdvisor().getProxyBucketRegion().getId();
  }

  @Override
  protected void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients,
      CacheWriter localWriter, boolean requireOldValue, Object expectedOldValue)
      throws CacheWriterException, TimeoutException {

    boolean origRemoteState = false;
    try {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        origRemoteState = event.isOriginRemote();
        event.setOriginRemote(true);
      }
      event.setRegion(this.partitionedRegion);
      this.partitionedRegion.cacheWriteBeforePut(event, netWriteRecipients, localWriter,
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
    boolean ret = false;
    try {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        origRemoteState = event.isOriginRemote();
        event.setOriginRemote(true);
      }
      event.setRegion(this.partitionedRegion);
      ret = this.partitionedRegion.cacheWriteBeforeDestroy(event, expectedOldValue);
    } finally {
      if (event.getPartitionMessage() != null || event.hasClientOrigin()) {
        event.setOriginRemote(origRemoteState);
      }
      event.setRegion(this);
    }
    return ret;
    // return super.cacheWriteBeforeDestroy(event);
  }

  @Override
  public CacheWriter basicGetWriter() {
    return this.partitionedRegion.basicGetWriter();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.partitioned.Bucket#getBucketOwners()
   * 
   * @since GemFire 5.9
   */
  public Set getBucketOwners() {
    return getBucketAdvisor().getProxyBucketRegion().getBucketOwners();
  }

  public long getCounter() {
    return counter.get();
  }

  public void setCounter(AtomicLong counter) {
    this.counter = counter;
  }

  public void updateCounter(long delta) {
    if (delta != 0) {
      this.counter.getAndAdd(delta);
    }
  }

  public void resetCounter() {
    if (this.counter.get() != 0) {
      this.counter.set(0);
    }
  }

  public long getLimit() {
    if (this.limit == null) {
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

  static int calcMemSize(Object value) {
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
  protected void updateSizeOnClearRegion(int sizeBeforeClear) {
    // This method is only called when the bucket is destroyed. If we
    // start supporting clear of partitioned regions, this logic needs to change
    // we can't just set these counters to zero, because there could be
    // concurrent operations that are also updating these stats. For example,
    // a destroy could have already been applied to the map, and then updates
    // the stat after we reset it, making the state negative.

    final PartitionedRegionDataStore prDs = this.partitionedRegion.getDataStore();
    long oldMemValue;

    if (this.isDestroyed || this.isDestroyingDiskRegion) {
      // If this region is destroyed, mark the stat as destroyed.
      oldMemValue = this.bytesInMemory.getAndSet(BUCKET_DESTROYED);

    } else if (!this.isInitialized()) {
      // This case is rather special. We clear the region if the GII failed.
      // In the case of bucket regions, we know that there will be no concurrent operations
      // if GII has failed, because there is not primary. So it's safe to set these
      // counters to 0.
      oldMemValue = this.bytesInMemory.getAndSet(0);
    }

    else {
      throw new InternalGemFireError(
          "Trying to clear a bucket region that was not destroyed or in initialization.");
    }
    if (oldMemValue != BUCKET_DESTROYED) {
      this.partitionedRegion.getPrStats().incDataStoreEntryCount(-sizeBeforeClear);
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
    return calcMemSize(regionEntry._getValue()); // OFFHEAP _getValue ok
  }

  @Override
  void updateSizeOnPut(Object key, int oldSize, int newSize) {
    updateBucket2Size(oldSize, newSize, SizeOp.UPDATE);
  }

  @Override
  void updateSizeOnCreate(Object key, int newSize) {
    this.partitionedRegion.getPrStats().incDataStoreEntryCount(1);
    updateBucket2Size(0, newSize, SizeOp.CREATE);
  }

  @Override
  void updateSizeOnRemove(Object key, int oldSize) {
    this.partitionedRegion.getPrStats().incDataStoreEntryCount(-1);
    updateBucket2Size(oldSize, 0, SizeOp.DESTROY);
  }

  @Override
  public int updateSizeOnEvict(Object key, int oldSize) {
    int newDiskSize = oldSize;
    updateBucket2Size(oldSize, newDiskSize, SizeOp.EVICT);
    return newDiskSize;
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
  protected void setMemoryThresholdFlag(MemoryEvent event) {
    Assert.assertTrue(false);
    // Bucket regions are not registered with ResourceListener,
    // and should not get this event
  }

  @Override
  public void initialCriticalMembers(boolean localHeapIsCritical,
      Set<InternalDistributedMember> criticalMembers) {
    // The owner Partitioned Region handles critical threshold events
  }

  @Override
  protected void closeCallbacksExceptListener() {
    // closeCacheCallback(getCacheLoader()); - fix bug 40228 - do NOT close loader
    closeCacheCallback(getCacheWriter());
    closeCacheCallback(getEvictionController());
  }

  public long getTotalBytes() {
    long result = this.bytesInMemory.get();
    if (result == BUCKET_DESTROYED) {
      return 0;
    }
    result += getNumOverflowBytesOnDisk();
    return result;
  }

  public long getBytesInMemory() {
    long result = this.bytesInMemory.get();
    if (result == BUCKET_DESTROYED) {
      return 0;
    }

    return result;
  }


  public void preDestroyBucket(int bucketId) {}

  @Override
  public void cleanupFailedInitialization() {
    this.preDestroyBucket(this.getId());
    super.cleanupFailedInitialization();
  }

  protected void invokePartitionListenerAfterBucketRemoved() {
    PartitionListener[] partitionListeners = getPartitionedRegion().getPartitionListeners();
    if (partitionListeners == null || partitionListeners.length == 0) {
      return;
    }
    for (int i = 0; i < partitionListeners.length; i++) {
      PartitionListener listener = partitionListeners[i];
      if (listener != null) {
        listener.afterBucketRemoved(getId(), keySet());
      }
    }
  }

  protected void invokePartitionListenerAfterBucketCreated() {
    PartitionListener[] partitionListeners = getPartitionedRegion().getPartitionListeners();
    if (partitionListeners == null || partitionListeners.length == 0) {
      return;
    }
    for (int i = 0; i < partitionListeners.length; i++) {
      PartitionListener listener = partitionListeners[i];
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
  void updateBucket2Size(int oldSize, int newSize, SizeOp op) {

    final int memoryDelta = op.computeMemoryDelta(oldSize, newSize);

    if (memoryDelta == 0)
      return;
    // do the bigger one first to keep the sum > 0
    updateBucketMemoryStats(memoryDelta);
  }

  void updateBucketMemoryStats(final int memoryDelta) {
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

    final PartitionedRegionDataStore prDS = this.partitionedRegion.getDataStore();
    prDS.updateMemoryStats(memoryDelta);
  }

  /**
   * Returns the current number of entries whose value has been overflowed to disk by this
   * bucket.This value will decrease when a value is faulted in.
   */
  public long getNumOverflowOnDisk() {
    return this.numOverflowOnDisk.get();
  }

  public long getNumOverflowBytesOnDisk() {
    return this.numOverflowBytesOnDisk.get();
  }

  /**
   * Returns the current number of entries whose value resides in the VM for this bucket. This value
   * will decrease when the entry is overflowed to disk.
   */
  public long getNumEntriesInVM() {
    return this.numEntriesInVM.get();
  }

  /**
   * Increments the current number of entries whose value has been overflowed to disk by this
   * bucket, by a given amount.
   */
  public void incNumOverflowOnDisk(long delta) {
    this.numOverflowOnDisk.addAndGet(delta);
  }

  public void incNumOverflowBytesOnDisk(long delta) {
    if (delta == 0)
      return;
    this.numOverflowBytesOnDisk.addAndGet(delta);
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
    this.numEntriesInVM.addAndGet(delta);
  }

  public void incEvictions(long delta) {
    this.evictions.getAndAdd(delta);
  }

  public long getEvictions() {
    return this.evictions.get();
  }

  @Override
  protected boolean isMemoryThresholdReachedForLoad() {
    return getBucketAdvisor().getProxyBucketRegion().isBucketSick();
  }

  public int getSizeForEviction() {
    EvictionAttributes ea = this.getAttributes().getEvictionAttributes();
    if (ea == null)
      return 0;
    EvictionAlgorithm algo = ea.getAlgorithm();
    if (!algo.isLRUHeap())
      return 0;
    EvictionAction action = ea.getAction();
    int size =
        action.isLocalDestroy() ? this.getRegionMap().sizeInVM() : (int) this.getNumEntriesInVM();
    return size;
  }

  @Override
  public HashMap getDestroyedSubregionSerialNumbers() {
    return new HashMap(0);
  }

  @Override
  public FilterProfile getFilterProfile() {
    return this.partitionedRegion.getFilterProfile();
  }

  @Override
  public void setCloningEnabled(boolean isCloningEnabled) {
    this.partitionedRegion.setCloningEnabled(isCloningEnabled);
  }

  @Override
  public boolean getCloningEnabled() {
    return this.partitionedRegion.getCloningEnabled();
  }

  @Override
  protected void generateLocalFilterRouting(InternalCacheEvent event) {
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
  public void beforeReleasingPrimaryLockDuringDemotion() {}

  @Override
  public RegionAttributes getAttributes() {
    return this;
  }

  public boolean areSecondariesPingable() {

    Set<InternalDistributedMember> hostingservers =
        this.partitionedRegion.getRegionAdvisor().getBucketOwners(this.getId());
    hostingservers.remove(cache.getDistributedSystem().getDistributedMember());

    if (cache.getLoggerI18n().fineEnabled())
      cache.getLoggerI18n()
          .fine("Pinging secondaries of bucket " + this.getId() + " on servers " + hostingservers);

    if (hostingservers.size() == 0)
      return true;

    return ServerPingMessage.send(cache, hostingservers);

  }

  @Override
  public boolean notifiesMultipleSerialGateways() {
    return getPartitionedRegion().notifiesMultipleSerialGateways();
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
      stopper.checkCancelInProgress(ie);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void postDestroyRegion(boolean destroyDiskRegion, RegionEventImpl event) {
    DiskRegion dr = this.getDiskRegion();
    if (dr != null && destroyDiskRegion) {
      dr.statsClear(this);
    }
    super.postDestroyRegion(destroyDiskRegion, event);
  }
}

