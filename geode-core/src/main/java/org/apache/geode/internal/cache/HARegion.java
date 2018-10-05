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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.event.NonDistributedEventTracker;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * This region is being implemented to suppress distribution of puts and to allow localDestroys on
 * mirrored regions.
 *
 * @since GemFire 4.3
 *
 */
public class HARegion extends DistributedRegion {
  private static final Logger logger = LogService.getLogger();

  CachePerfStats haRegionStats;

  // Prevent this region from participating in a TX, bug 38709
  @Override
  public boolean isSecret() {
    return true;
  }

  @Override
  public boolean isCopyOnRead() {
    return false;
  }

  @Override
  public boolean doesNotDistribute() {
    return true;
  }

  @Override
  protected StringBuilder getStringBuilder() {
    StringBuilder buf = new StringBuilder();
    buf.append("HARegion");
    buf.append("[path='").append(getFullPath());
    return buf;
  }

  // protected Object conditionalCopy(Object o) {
  // return o;
  // }

  private volatile HARegionQueue owningQueue;

  private HARegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache) {
    super(regionName, attrs, parentRegion, cache,
        new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
            .setSnapshotInputStream(null).setImageTarget(null));
    this.haRegionStats = new DummyCachePerfStats();
  }

  @Override
  public boolean allowsPersistence() {
    return false;
  }

  /**
   * Updates never distributed from buckets.
   *
   * @since GemFire 5.7
   */
  @Override
  protected void distributeUpdate(EntryEventImpl event, long lastModifiedTime, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue) {}

  @Override
  public EventTracker createEventTracker() {
    // event trackers aren't needed for HARegions
    return NonDistributedEventTracker.getInstance();
  }

  /**
   * void implementation over-riding the method to allow localDestroy on mirrored regions
   *
   */
  @Override
  protected void checkIfReplicatedAndLocalDestroy(EntryEventImpl event) {}

  @Override
  void checkEntryTimeoutAction(String mode, ExpirationAction ea) {}

  /**
   * Overriding this method so as to allow expiry action of local invalidate even if the scope is
   * distributed mirrored.
   *
   * <p>
   * author Asif
   */
  @Override
  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive) {
    // checkReadiness();
    if (timeToLive == null) {
      throw new IllegalArgumentException(
          "timeToLive must not be null");
    }
    if ((timeToLive.getAction() == ExpirationAction.LOCAL_DESTROY
        && this.getDataPolicy().withReplication())) {
      throw new IllegalArgumentException(
          "timeToLive action is incompatible with this region's mirror type");
    }
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set time to live when statistics are disabled");
    }
    ExpirationAttributes oldAttrs = getEntryTimeToLive();
    this.entryTimeToLive = timeToLive.getTimeout();
    this.entryTimeToLiveExpirationAction = timeToLive.getAction();
    setEntryTimeToLiveAttributes();
    updateEntryExpiryPossible();
    timeToLiveChanged(oldAttrs);
    return oldAttrs;
  }

  /**
   * Before invalidating , check if the entry being invalidated has a key as Long . If yes check if
   * the key is still present in availableIDs . If yes remove & allow invalidation to proceed. But
   * if the key (Long)is absent do not allow invalidation to proceed.
   *
   * <p>
   * author Asif
   */
  @Override
  protected void basicInvalidate(final EntryEventImpl event, boolean invokeCallbacks,
      final boolean forceNewEntry) throws EntryNotFoundException {
    Object key = event.getKey();
    if (key instanceof Long) {
      boolean removedFromAvID = false;
      Conflatable conflatable = null;
      try {
        conflatable = (Conflatable) this.get(key);
        removedFromAvID =
            !this.owningQueue.isPrimary() && this.owningQueue.destroyFromAvailableIDs((Long) key);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        getCancelCriterion().checkCancelInProgress(ie);
        return;
      }
      if (!removedFromAvID) {
        return;
      }

      // <HA overflow>
      if (conflatable instanceof HAEventWrapper) {
        this.owningQueue.decAndRemoveFromHAContainer((HAEventWrapper) conflatable, "Invalidate");
      }
      // </HA overflow>
      // update the stats
      this.owningQueue.stats.incEventsExpired();
    }
    this.entries.invalidate(event, invokeCallbacks, forceNewEntry, false);
    return;

  }

  /**
   * This method is over-ridden since we do not want GII of ThreadIdentifier objects to happen
   */
  @Override
  protected boolean checkEntryNotValid(RegionEntry mapEntry) {
    return (super.checkEntryNotValid(mapEntry) || mapEntry.getKey() instanceof ThreadIdentifier);
  }

  @Override
  public Object put(Object key, Object value, Object aCallbackArgument)
      throws TimeoutException, CacheWriterException {
    checkReadiness();

    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.UPDATE, key, value,
        aCallbackArgument, false, getMyId());
    try {

      Object oldValue = null;

      if (basicPut(event, false, // ifNew
          false, // ifOld
          null, // expectedOldValue
          false // requireOldValue
      )) {
        oldValue = event.getOldValue();
      }
      return handleNotAvailable(oldValue);
    } finally {
      event.release();
    }
  }

  /**
   *
   * Returns an instance of HARegion after it has properly initialized
   *
   * @param regionName name of the region to be created
   * @param cache the cache that owns this region
   * @param ra attributes of the region
   * @return an instance of an HARegion
   * @throws RegionExistsException if a region of the same name exists in the same Cache
   */
  public static HARegion getInstance(String regionName, InternalCache cache, HARegionQueue hrq,
      RegionAttributes ra)
      throws TimeoutException, RegionExistsException, IOException, ClassNotFoundException {

    HARegion haRegion = new HARegion(regionName, ra, null, cache);
    haRegion.setOwner(hrq);
    Region region = cache.createVMRegion(regionName, ra,
        new InternalRegionArguments().setInternalMetaRegion(haRegion).setDestroyLockFlag(true)
            .setSnapshotInputStream(null).setInternalRegion(true).setImageTarget(null));

    return (HARegion) region;
  }

  public boolean isPrimaryQueue() {
    if (this.owningQueue != null) {
      return this.owningQueue.isPrimary();
    }
    return false;
  }

  public HARegionQueue getOwner() {
    // fix for bug #41634 - don't release a reference to the owning queue until
    // it is fully initialized. The previous implementation of this rule did
    // not protect subclasses of HARegionQueue and caused the bug.
    return this.owningQueue.isQueueInitialized() ? this.owningQueue : null;
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    return this.haRegionStats;
  }

  /**
   * This method is used to set the HARegionQueue owning the HARegion. It is set after the
   * HARegionQueue is properly constructed
   *
   * @param hrq The owning HARegionQueue instance
   */
  public void setOwner(HARegionQueue hrq) {
    this.owningQueue = hrq;
  }

  @Override
  public boolean shouldNotifyBridgeClients() {
    return false;
  }

  // re-implemented from LocalRegion to avoid recording the event in GemFireCache
  // before it's applied to the cache's region
  // public boolean hasSeenClientEvent(InternalCacheEvent event) {
  // return false;
  // }

  protected void notifyGatewayHub(EnumListenerEvent operation, EntryEventImpl event) {}

  /**
   * This method is overriden so as to make isOriginRemote true always so that the operation is
   * never propagated to other nodes
   *
   * @see org.apache.geode.internal.cache.AbstractRegion#destroyRegion()
   */
  @Override
  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    // Do not generate EventID
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_DESTROY, aCallbackArgument,
        true /* isOriginRemote */, getMyId());

    basicDestroyRegion(event, true);
  }

  /**
   * Never genearte EventID for any Entry or Region operation on the HARegion
   */
  @Override
  public boolean generateEventID() {
    return false;
  }

  @Override
  public void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException {
    // Set this region in the ProxyBucketRegion early so that profile exchange will
    // perform the correct fillInProfile method
    // try {
    super.initialize(snapshotInputStream, imageTarget, internalRegionArgs);
    // } finally {
    // this.giiProviderStates = null;
    // }
  }

  /**
   * @return the deserialized value
   * @see LocalRegion#findObjectInSystem(KeyInfo, boolean, TXStateInterface, boolean, Object,
   *      boolean, boolean, ClientProxyMembershipID, EntryEventImpl, boolean)
   *
   */
  @Override
  protected Object findObjectInSystem(KeyInfo keyInfo, boolean isCreate, TXStateInterface txState,
      boolean generateCallbacks, Object localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws CacheLoaderException, TimeoutException {

    Object value = null;
    final Object key = keyInfo.getKey();
    final Object aCallbackArgument = keyInfo.getCallbackArg();
    // copy into local var to prevent race condition
    RegionEntry re = null;
    Assert.assertTrue(!hasServerProxy());
    CacheLoader loader = basicGetLoader();
    if (loader != null) {
      value = callCacheLoader(loader, key, aCallbackArgument, preferCD);

      if (value != null) {
        try {
          validateKey(key);
          Operation op;
          if (isCreate) {
            op = Operation.LOCAL_LOAD_CREATE;
          } else {
            op = Operation.LOCAL_LOAD_UPDATE;
          }

          @Released
          EntryEventImpl event = EntryEventImpl.create(this, op, key, value, aCallbackArgument,
              false, getMyId(), generateCallbacks);
          try {
            re = basicPutEntry(event, 0L);
          } finally {
            event.release();
          }
          if (txState == null) {
          }
        } catch (CacheWriterException cwe) {
          // @todo smenon Log the exception
        }
      }
    }
    if (isCreate) {
      recordMiss(re, key);
    }
    return value;
  }

  /**
   * invoked when we start providing a GII image
   */
  public void startServingGIIRequest() {
    // some of our dunit tests create HARegions in odd ways that cause owningQueue
    // to be null during GII
    if (this.owningQueue == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "found that owningQueue was null during GII of {} which could lead to event loss (see #41681)",
            this);
      }
      return;
    }
    this.owningQueue.startGiiQueueing();
  }

  /**
   * invoked when we finish providing a GII image
   */
  public void endServingGIIRequest() {
    if (this.owningQueue != null) {
      this.owningQueue.endGiiQueueing();
    }
  }

  @Override
  protected CacheDistributionAdvisor createDistributionAdvisor(
      InternalRegionArguments internalRegionArgs) {
    return HARegionAdvisor.createHARegionAdvisor(this); // Warning: potential early escape of object
                                                        // before full construction
  }

  @Override
  public void fillInProfile(Profile profile) {
    super.fillInProfile(profile);
    HARegionAdvisor.HAProfile h = (HARegionAdvisor.HAProfile) profile;
    // dunit tests create HARegions without encapsulating them in queues
    if (this.owningQueue != null) {
      h.isPrimary = this.owningQueue.isPrimary();
      h.hasRegisteredInterest = this.owningQueue.getHasRegisteredInterest();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.LocalRegion#getEventState()
   */
  @Override
  public Map<? extends DataSerializable, ? extends DataSerializable> getEventState() {
    if (this.owningQueue != null) {
      return this.owningQueue.getEventMapForGII();
    }
    return null;
  }

  /*
   * Record cache event state for a potential initial image provider. This is used to install event
   * state when the sender is selected as initial image provider.
   *
   *
   */
  @Override
  public void recordEventState(InternalDistributedMember sender, Map eventState) {
    if (eventState != null && this.owningQueue != null) {
      this.owningQueue.recordEventState(sender, eventState);
    }
  }


  /** send a distribution advisor profile update to other members */
  public void sendProfileUpdate() {
    new UpdateAttributesProcessor(this).distribute(false);
  }


  /**
   * whether the primary queue for the client has registered interest, or there is no primary
   * present
   */
  public boolean noPrimaryOrHasRegisteredInterest() {
    return ((HARegionAdvisor) this.distAdvisor).noPrimaryOrHasRegisteredInterest();
  }

  public Object updateHAEventWrapper(InternalDistributedMember sender,
      CachedDeserializable newValueCd) {
    return this.owningQueue.updateHAEventWrapper(sender, newValueCd, getName());
  }

  /** HARegions have their own advisors so that interest registration state can be tracked */
  public static class HARegionAdvisor extends CacheDistributionAdvisor {
    private HARegionAdvisor(CacheDistributionAdvisee region) {
      super(region);
    }

    public static HARegionAdvisor createHARegionAdvisor(CacheDistributionAdvisee region) {
      HARegionAdvisor advisor = new HARegionAdvisor(region);
      advisor.initialize();
      return advisor;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.internal.cache.CacheDistributionAdvisor#adviseInitialImage(org.apache.geode.
     * internal.cache.CacheDistributionAdvisor.InitialImageAdvice)
     */
    @Override
    public InitialImageAdvice adviseInitialImage(InitialImageAdvice previousAdvice) {
      InitialImageAdvice r = super.adviseInitialImage(previousAdvice);
      r.setOthers(this.getAdvisee().getDistributionManager().getOtherDistributionManagerIds());
      return r;
    }

    @Override
    protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
      return new HAProfile(memberId, version);
    }

    public boolean noPrimaryOrHasRegisteredInterest() {
      Profile[] locProfiles = this.profiles; // grab current profiles
      for (int i = 0; i < locProfiles.length; i++) {
        HAProfile p = (HAProfile) locProfiles[i];
        if (p.isPrimary) {
          return p.hasRegisteredInterest;
        }
      }
      // if no primary, we want to accept events
      return true;
    }

    public static class HAProfile extends CacheProfile {
      private static int HAS_REGISTERED_INTEREST_BIT = 0x01;
      private static int IS_PRIMARY_BIT = 0x02;

      boolean hasRegisteredInterest;

      boolean isPrimary;

      public HAProfile() {
        // for deserialization only
      }

      public HAProfile(InternalDistributedMember memberId, int version) {
        super(memberId, version);
      }

      /*
       * (non-Javadoc)
       *
       * @see
       * org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile#fromData(java.io.
       * DataInput)
       */
      @Override
      public void fromData(DataInput in) throws IOException, ClassNotFoundException {
        super.fromData(in);
        int flags = in.readByte();
        hasRegisteredInterest = (flags & HAS_REGISTERED_INTEREST_BIT) != 0;
        isPrimary = (flags & IS_PRIMARY_BIT) != 0;
      }

      /*
       * (non-Javadoc)
       *
       * @see org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile#toData(java.io.
       * DataOutput)
       */
      @Override
      public void toData(DataOutput out) throws IOException {
        super.toData(out);
        int flags = 0;
        if (hasRegisteredInterest) {
          flags |= HAS_REGISTERED_INTEREST_BIT;
        }
        if (isPrimary) {
          flags |= IS_PRIMARY_BIT;
        }
        out.writeByte(flags & 0xff);
      }

      @Override
      public int getDSFID() {
        return HA_PROFILE;
      }

      @Override
      public StringBuilder getToStringHeader() {
        return new StringBuilder("HAProfile");
      }

      @Override
      public void fillInToString(StringBuilder sb) {
        super.fillInToString(sb);
        sb.append("; isPrimary=").append(this.isPrimary);
        sb.append("; hasRegisteredInterest=").append(this.hasRegisteredInterest);
      }
    }
  }
}
