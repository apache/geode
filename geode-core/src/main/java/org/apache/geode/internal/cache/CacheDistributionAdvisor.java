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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Adds bookkeeping info and cache-specific behavior to DistributionAdvisor. Adds bit-encoded flags
 * in addition to object info.
 *
 */
@SuppressWarnings("deprecation")
public class CacheDistributionAdvisor extends DistributionAdvisor {

  private static final Logger logger = LogService.getLogger();

  // moved ROLLOVER_* constants to DistributionAdvisor

  /** bit masks */
  private static final int INTEREST_MASK = 0x01;
  private static final int REPLICATE_MASK = 0x02;
  private static final int LOADER_MASK = 0x04;
  private static final int WRITER_MASK = 0x08;
  private static final int LISTENER_MASK = 0x10;
  private static final int DIST_ACK_MASK = 0x20;
  private static final int GLOBAL_MASK = 0x40;
  private static final int IN_RECOVERY_MASK = 0x80;
  private static final int PERSISTENT_MASK = 0x100;
  private static final int PROXY_MASK = 0x200;
  private static final int PRELOADED_MASK = 0x400;
  private static final int IS_PARTITIONED_MASK = 0x800;
  private static final int REGION_INITIALIZED_MASK = 0x1000;
  private static final int IS_GATEWAY_ENABLED_MASK = 0x2000;
  // provider is no longer a supported attribute.
  // private static final int IS_GII_PROVIDER_MASK = 0x4000;
  private static final int PERSISTENT_ID_MASK = 0x4000;
  /** does this member require operation notification (PartitionedRegions) */
  protected static final int REQUIRES_NOTIFICATION_MASK = 0x8000;
  private static final int HAS_CACHE_SERVER_MASK = 0x10000;
  private static final int REQUIRES_OLD_VALUE_MASK = 0x20000;
  // unused 0x40000;
  private static final int PERSISTENCE_INITIALIZED_MASK = 0x80000;
  // Important below mentioned bit masks are not available
  /**
   * Using following masks for gatewaysender queue startup polic informations.
   */
  // private static final int HUB_STARTUP_POLICY_MASK = 0x07<<20;

  private static final int GATEWAY_SENDER_IDS_MASK = 0x200000;

  private static final int ASYNC_EVENT_QUEUE_IDS_MASK = 0x400000;
  private static final int IS_OFF_HEAP_MASK = 0x800000;
  private static final int CACHE_SERVICE_PROFILES_MASK = 0x1000000;


  // moved initializ* to DistributionAdvisor

  // moved membershipVersion to DistributionAdvisor

  // moved previousVersionOpCount to DistributionAdvisor
  // moved currentVersionOpCount to DistributionAdvisor

  // moved removedProfiles to DistributionAdvisor

  /** Creates a new instance of CacheDistributionAdvisor */
  protected CacheDistributionAdvisor(CacheDistributionAdvisee region) {
    super(region);
  }

  static CacheDistributionAdvisor createCacheDistributionAdvisor(
      CacheDistributionAdvisee region) {
    CacheDistributionAdvisor advisor = new CacheDistributionAdvisor(region);
    advisor.initialize();
    return advisor;
  }

  @Override
  public String toString() {
    return "CacheDistributionAdvisor for region " + getAdvisee().getFullPath();
  }

  // moved toStringWithProfiles to DistributionAdvisor

  // moved initializationGate to DistributionAdvisor

  // moved isInitialized to DistributionAdvisor

  // moved addMembershipListenerAndAdviseGeneric to DistributionAdvisor

  /**
   * Returns a the set of members that either want all events or are caching data.
   *
   * @param excludeInRecovery if true then members in recovery are excluded
   */
  private Set<InternalDistributedMember> adviseAllEventsOrCached(final boolean excludeInRecovery)
      throws IllegalStateException {
    getAdvisee().getCancelCriterion().checkCancelInProgress(null);
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      if (excludeInRecovery && cp.inRecovery) {
        return false;
      }
      return cp.cachedOrAllEventsWithListener();
    });
  }

  /**
   * Provide recipient information for an update or create operation.
   *
   */
  Set adviseUpdate(final EntryEventImpl event) throws IllegalStateException {
    if (event.hasNewValue() || event.getOperation().isPutAll()) {
      // only need to distribute it to members that want all events or cache data
      return adviseAllEventsOrCached(true/* fixes 41147 */);
    } else {
      // The new value is null so this is a create with a null value,
      // in which case we only need to distribute this message to replicates
      // or all events that are not a proxy or if a proxy has a listener
      return adviseFilter(profile -> {
        assert profile instanceof CacheProfile;
        CacheProfile cp = (CacheProfile) profile;
        DataPolicy dp = cp.dataPolicy;
        return dp.withReplication()
            || (cp.allEvents() && (dp.withStorage() || cp.hasCacheListener));
      });
    }
  }

  /**
   * Provide recipient information for TX lock and commit.
   *
   * @return Set of Serializable members that the current transaction will be distributed to.
   *         Currently this is any other member who has this region defined. No reference to Set
   *         kept by advisor so caller is free to modify it
   */
  Set<InternalDistributedMember> adviseTX() throws IllegalStateException {

    boolean isMetaDataWithTransactions = getAdvisee() instanceof LocalRegion
        && ((LocalRegion) getAdvisee()).isMetaRegionWithTransactions();

    Set<InternalDistributedMember> badList = Collections.emptySet();
    if (!TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS && !isMetaDataWithTransactions) {
      badList = adviseFilter(profile -> {
        assert profile instanceof CacheProfile;
        CacheProfile prof = (CacheProfile) profile;
        return (prof.isPersistent());
      });
    }
    if (badList.isEmpty()) {
      return adviseFilter(profile -> {
        assert profile instanceof CacheProfile;
        CacheProfile cp = (CacheProfile) profile;
        return cp.cachedOrAllEvents();
      });
    } else {
      StringBuilder badIds = new StringBuilder();
      Iterator biI = badList.iterator();
      while (biI.hasNext()) {
        badIds.append(biI.next().toString());
        if (biI.hasNext()) {
          badIds.append(", ");
        }
      }
      throw new IllegalStateException(
          String.format("Illegal Region Configuration for members: %s",
              badIds.toString()));
    }
  }

  /**
   * Provide recipient information for netLoad
   *
   * @return Set of Serializable members that have a CacheLoader installed; no reference to Set kept
   *         by advisor so caller is free to modify it
   */
  public Set adviseNetLoad() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile prof = (CacheProfile) profile;

      // if region in cache is not yet initialized, exclude
      if (!prof.regionInitialized) { // fix for bug 41102
        return false;
      }

      return prof.hasCacheLoader;
    });
  }

  public FilterRoutingInfo adviseFilterRouting(CacheEvent event, Set cacheOpRecipients) {
    FilterProfile fp = ((LocalRegion) event.getRegion()).getFilterProfile();
    if (fp != null) {
      return fp.getFilterRoutingInfoPart1(event, profiles, cacheOpRecipients);
    }
    return null;
  }


  /**
   * Same as adviseGeneric except in recovery excluded.
   */
  public Set<InternalDistributedMember> adviseCacheOp() {
    return adviseAllEventsOrCached(true);
  }

  /*
   * * Same as adviseGeneric but excludes if cache profile is in recovery
   */
  Set<InternalDistributedMember> adviseInvalidateRegion() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return !cp.inRecovery;
    });
  }


  /**
   * Same as adviseGeneric
   */
  public Set adviseDestroyRegion() {
    return adviseGeneric();
  }

  /**
   * Provide recipient information for netWrite
   *
   * @return Set of Serializable member ids that have a CacheWriter installed; no reference to Set
   *         kept by advisor so caller is free to modify it
   */
  public Set adviseNetWrite() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile prof = (CacheProfile) profile;
      // if region in cache is in recovery, exclude
      if (prof.inRecovery) {
        return false;
      }

      return prof.hasCacheWriter;
    });
  }

  public Set<InternalDistributedMember> adviseInitializedReplicates() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.dataPolicy.withReplication() && cp.regionInitialized;
    });
  }

  /**
   * Provide recipient information for netSearch
   *
   * @return Set of Serializable member ids that have the region and are have storage (no need to
   *         search an empty cache)
   */
  Set adviseNetSearch() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      // if region in cache is not yet initialized, exclude
      if (!cp.regionInitialized) {
        return false;
      }
      DataPolicy dp = cp.dataPolicy;
      return dp.withStorage();
    });
  }

  // moved dumpProfiles to DistributionAdvisor

  public InitialImageAdvice adviseInitialImage(InitialImageAdvice previousAdvice) {
    return adviseInitialImage(previousAdvice, false);
  }

  @SuppressWarnings("synthetic-access")
  public InitialImageAdvice adviseInitialImage(InitialImageAdvice previousAdvice,
      boolean persistent) {
    initializationGate();

    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      dumpProfiles("AdviseInitialImage");
    }

    Profile[] allProfiles = profiles; // volatile read
    if (allProfiles.length == 0) {
      return new InitialImageAdvice();
    }

    Set<InternalDistributedMember> replicates = new HashSet<>();
    Set<InternalDistributedMember> others = new HashSet<>();
    Set<InternalDistributedMember> preloaded = new HashSet<>();
    Set<InternalDistributedMember> empties = new HashSet<>();
    Set<InternalDistributedMember> uninitialized = new HashSet<>();
    Set<InternalDistributedMember> nonPersistent = new HashSet<>();

    Map<InternalDistributedMember, CacheProfile> memberProfiles =
        new HashMap<>();

    for (Profile allProfile : allProfiles) {
      CacheProfile profile = (CacheProfile) allProfile;

      // Make sure that we don't return a member that was in the previous initial image advice.
      // Unless that member has changed it's profile since the last time we checked.
      if (previousAdvice != null) {
        CacheProfile previousProfile =
            previousAdvice.memberProfiles.get(profile.getDistributedMember());
        if (previousProfile != null
            && previousProfile.getSerialNumber() == profile.getSerialNumber()
            && previousProfile.getVersion() == profile.getVersion()) {
          continue;
        }
      }

      // if region in cache is in recovery, exclude
      if (profile.inRecovery) {
        uninitialized.add(profile.getDistributedMember());
        continue;
      }
      // No need to do a GII from uninitialized member.
      if (!profile.regionInitialized) {
        uninitialized.add(profile.getDistributedMember());
        continue;
      }

      if (profile.dataPolicy.withReplication()) {
        if (!persistent || profile.dataPolicy.withPersistence()) {
          // If the local member is persistent, we only want
          // to include persistent replicas in the set of replicates.
          replicates.add(profile.getDistributedMember());
        } else {
          nonPersistent.add(profile.getDistributedMember());
        }
        memberProfiles.put(profile.getDistributedMember(), profile);
      } else if (profile.dataPolicy.isPreloaded()) {
        preloaded.add(profile.getDistributedMember());
        memberProfiles.put(profile.getDistributedMember(), profile);
      } else if (profile.dataPolicy.withStorage()) {
        // don't bother asking proxy members for initial image
        others.add(profile.getDistributedMember());
        memberProfiles.put(profile.getDistributedMember(), profile);
      } else {
        empties.add(profile.getDistributedMember());
      }
    }

    InitialImageAdvice advice = new InitialImageAdvice(replicates, others, preloaded, empties,
        uninitialized, nonPersistent, memberProfiles);

    if (logger.isDebugEnabled()) {
      logger.debug(advice);
    }
    return advice;
  }

  /**
   * returns the set of all the members in the system which requires old values and are not yet
   * finished with initialization (including GII).
   *
   * @since GemFire 5.5
   */
  Set adviseRequiresOldValueInCacheOp() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.requiresOldValueInEvents && !cp.regionInitialized;
    });
  }


  // moved adviseProfileExchange to DistributionAdvisor

  // moved getProfile to DistributionAdvisor

  // moved exchangeProfiles to DistributionAdvisor

  // moved getDistributionManager to DistributionAdvisor

  /** Instantiate new distribution profile for this member */
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new CacheProfile(memberId, version);
  }

  @Override
  protected boolean evaluateProfiles(Profile newProfile, Profile oldProfile) {
    boolean result = super.evaluateProfiles(newProfile, oldProfile);
    if (result) {
      CacheProfile newCP = (CacheProfile) newProfile;
      CacheProfile oldCP = (CacheProfile) oldProfile;
      if ((oldCP == null || !oldCP.regionInitialized) && newCP.regionInitialized) {
        // invoke membership listeners, if any
        CacheDistributionAdvisee advisee = (CacheDistributionAdvisee) getAdvisee();
        advisee.remoteRegionInitialized(newCP);
      }
    }
    return result;
  }

  /**
   * Profile information for a remote counterpart.
   */
  public static class CacheProfile extends DistributionAdvisor.Profile {
    public DataPolicy dataPolicy = DataPolicy.REPLICATE;
    public InterestPolicy interestPolicy = InterestPolicy.DEFAULT;
    public boolean hasCacheLoader = false;
    public boolean hasCacheWriter = false;
    public boolean hasCacheListener = false;
    public Scope scope = Scope.DISTRIBUTED_NO_ACK;
    public boolean inRecovery = false;
    public Set<String> gatewaySenderIds = Collections.emptySet();
    public Set<String> asyncEventQueueIds = Collections.emptySet();
    /**
     * Will be null if the profile doesn't need to have the attributes
     */
    public SubscriptionAttributes subscriptionAttributes = null;

    public boolean isPartitioned = false;
    public boolean isGatewayEnabled = false;
    public boolean isPersistent = false;

    public boolean isOffHeap = false;

    // moved initialMembershipVersion to DistributionAdvisor.Profile
    // moved serialNumber to DistributionAdvisor.Profile

    /**
     * this member's client interest / continuous query profile. This is used for event processing
     * to reduce the number of times CQs are executed and to have the originating member for an
     * event pay the cpu cost of executing filters on the event.
     */
    public FilterProfile filterProfile;

    /**
     * Some cache listeners require old values in cache operation messages, at least during GII
     */
    boolean requiresOldValueInEvents;

    /**
     * Whether the region has completed initialization, including GII. This information may be
     * incorrect for a PartitionedRegion, but may be relied upon for DistributedRegions (including
     * BucketRegions)
     *
     * @since GemFire prpersist this field is now overloaded for partitioned regions with
     *        persistence. In the case of pr persistence, this field indicates that the region has
     *        finished recovery from disk.
     */
    public boolean regionInitialized;


    /**
     * True when a members persistent store is initialized. Note that regionInitialized may be true
     * when this is false in the case of createBucketAtomically. With createBucketAtomically, the
     * peristent store is not created until the EndBucketCreationMessage is sent.
     */
    public boolean persistenceInitialized;

    public PersistentMemberID persistentID;

    /**
     * This member has any cache servers. This is not actively maintained for local profiles (i.e.,
     * a profile representing this vm)
     */
    public boolean hasCacheServer = false;

    List<CacheServiceProfile> cacheServiceProfiles = new ArrayList<>();

    /** for internal use, required for DataSerializer.readObject */
    public CacheProfile() {}

    /** used for routing computation */
    public CacheProfile(FilterProfile localProfile) {
      filterProfile = localProfile;
    }

    public CacheProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public CacheProfile(CacheProfile toCopy) {
      super(toCopy.getDistributedMember(), toCopy.version);
      setIntInfo(toCopy.getIntInfo());
    }

    /** Return the profile data information that can be stored in an int */
    protected int getIntInfo() {
      int s = 0;
      if (dataPolicy.withReplication()) {
        s |= REPLICATE_MASK;
        if (dataPolicy.isPersistentReplicate()) {
          s |= PERSISTENT_MASK;
        }
      } else {
        if (dataPolicy.isEmpty()) {
          s |= PROXY_MASK;
        }
        if (dataPolicy.isPreloaded()) {
          s |= PRELOADED_MASK;
        }
      }
      if (subscriptionAttributes != null
          && subscriptionAttributes.getInterestPolicy().isAll()) {
        s |= INTEREST_MASK;
      }
      if (hasCacheLoader) {
        s |= LOADER_MASK;
      }
      if (hasCacheWriter) {
        s |= WRITER_MASK;
      }
      if (hasCacheListener) {
        s |= LISTENER_MASK;
      }
      if (scope.isDistributedAck()) {
        s |= DIST_ACK_MASK;
      }
      if (scope.isGlobal()) {
        s |= GLOBAL_MASK;
      }
      if (inRecovery) {
        s |= IN_RECOVERY_MASK;
      }
      if (isPartitioned) {
        s |= IS_PARTITIONED_MASK;
      }
      if (isGatewayEnabled) {
        s |= IS_GATEWAY_ENABLED_MASK;
      }
      if (isPersistent) {
        s |= PERSISTENT_MASK;
      }
      if (regionInitialized) {
        s |= REGION_INITIALIZED_MASK;
      }
      if (persistentID != null) {
        s |= PERSISTENT_ID_MASK;
      }
      if (hasCacheServer) {
        s |= HAS_CACHE_SERVER_MASK;
      }
      if (requiresOldValueInEvents) {
        s |= REQUIRES_OLD_VALUE_MASK;
      }
      if (persistenceInitialized) {
        s |= PERSISTENCE_INITIALIZED_MASK;
      }
      if (!gatewaySenderIds.isEmpty()) {
        s |= GATEWAY_SENDER_IDS_MASK;
      }
      if (!asyncEventQueueIds.isEmpty()) {
        s |= ASYNC_EVENT_QUEUE_IDS_MASK;
      }
      if (isOffHeap) {
        s |= IS_OFF_HEAP_MASK;
      }
      if (!cacheServiceProfiles.isEmpty()) {
        s |= CACHE_SERVICE_PROFILES_MASK;
      }
      Assert.assertTrue(!scope.isLocal());
      return s;
    }

    private boolean hasGatewaySenderIds(int bits) {
      return (bits & GATEWAY_SENDER_IDS_MASK) != 0;
    }

    private boolean hasAsyncEventQueueIds(int bits) {
      return (bits & ASYNC_EVENT_QUEUE_IDS_MASK) != 0;
    }

    /**
     * @return true if the serialized message has a persistentID
     */
    private boolean hasPersistentID(int bits) {
      return (bits & PERSISTENT_ID_MASK) != 0;
    }

    public boolean isPersistent() {
      return dataPolicy.withPersistence();
    }

    /** Set the profile data information that is stored in a short */
    protected void setIntInfo(int s) {
      if ((s & REPLICATE_MASK) != 0) {
        if ((s & PERSISTENT_MASK) != 0) {
          dataPolicy = DataPolicy.PERSISTENT_REPLICATE;
        } else {
          dataPolicy = DataPolicy.REPLICATE;
        }
      } else if ((s & PROXY_MASK) != 0) {
        dataPolicy = DataPolicy.EMPTY;
      } else if ((s & PRELOADED_MASK) != 0) {
        dataPolicy = DataPolicy.PRELOADED;
      } else { // CACHED
        dataPolicy = DataPolicy.NORMAL;
      }

      if ((s & IS_PARTITIONED_MASK) != 0) {
        if ((s & PERSISTENT_MASK) != 0) {
          dataPolicy = DataPolicy.PERSISTENT_PARTITION;
        } else {
          dataPolicy = DataPolicy.PARTITION;
        }
      }

      if ((s & INTEREST_MASK) != 0) {
        subscriptionAttributes = new SubscriptionAttributes(InterestPolicy.ALL);
      } else {
        subscriptionAttributes = new SubscriptionAttributes(InterestPolicy.CACHE_CONTENT);
      }
      hasCacheLoader = (s & LOADER_MASK) != 0;
      hasCacheWriter = (s & WRITER_MASK) != 0;
      hasCacheListener = (s & LISTENER_MASK) != 0;
      scope = (s & DIST_ACK_MASK) != 0 ? Scope.DISTRIBUTED_ACK
          : ((s & GLOBAL_MASK) != 0 ? Scope.GLOBAL : Scope.DISTRIBUTED_NO_ACK);
      inRecovery = (s & IN_RECOVERY_MASK) != 0;
      isPartitioned = (s & IS_PARTITIONED_MASK) != 0;
      isGatewayEnabled = (s & IS_GATEWAY_ENABLED_MASK) != 0;
      isPersistent = (s & PERSISTENT_MASK) != 0;
      regionInitialized = ((s & REGION_INITIALIZED_MASK) != 0);
      hasCacheServer = ((s & HAS_CACHE_SERVER_MASK) != 0);
      requiresOldValueInEvents = ((s & REQUIRES_OLD_VALUE_MASK) != 0);
      persistenceInitialized = (s & PERSISTENCE_INITIALIZED_MASK) != 0;
      isOffHeap = (s & IS_OFF_HEAP_MASK) != 0;
    }

    /**
     * Sets the SubscriptionAttributes for the region that this profile is on
     *
     * @since GemFire 5.0
     */
    public void setSubscriptionAttributes(SubscriptionAttributes sa) {
      subscriptionAttributes = sa;
    }

    /**
     * Return true if cached or allEvents and a listener
     */
    boolean cachedOrAllEventsWithListener() {
      // to fix bug 36804 to ignore hasCacheListener
      // return this.dataPolicy.withStorage() ||
      // (allEvents() && this.hasCacheListener);
      return cachedOrAllEvents();
    }

    /**
     * Return true if cached or allEvents
     */
    boolean cachedOrAllEvents() {
      return dataPolicy.withStorage() || allEvents();
    }

    /**
     * Return true if subscribed to all events
     */
    boolean allEvents() {
      return subscriptionAttributes.getInterestPolicy().isAll();
    }

    public void addCacheServiceProfile(CacheServiceProfile profile) {
      cacheServiceProfiles.add(profile);
    }

    private boolean hasCacheServiceProfiles(int bits) {
      return (bits & CACHE_SERVICE_PROFILES_MASK) != 0;
    }

    /**
     * Used to process an incoming cache profile.
     */
    @Override
    public void processIncoming(ClusterDistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      try {
        Assert.assertTrue(adviseePath != null, "adviseePath was null");

        InternalRegion lclRgn;
        final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
        try {
          InternalCache cache = dm.getCache();
          lclRgn = cache == null ? null : cache.getInternalRegionByPath(adviseePath);
        } finally {
          LocalRegion.setThreadInitLevelRequirement(oldLevel);
        }

        if (lclRgn instanceof CacheDistributionAdvisee) {
          if (lclRgn.isUsedForPartitionedRegionBucket()) {
            if (!((BucketRegion) lclRgn).isPartitionedRegionOpen()) {
              return;
            }
          }
          handleCacheDistributionAdvisee((CacheDistributionAdvisee) lclRgn, adviseePath,
              removeProfile, exchangeProfiles, true, replyProfiles);
        } else {
          if (lclRgn == null) {
            handleCacheDistributionAdvisee(
                PartitionedRegionHelper.getProxyBucketRegion(dm.getCache(), adviseePath, false),
                adviseePath, removeProfile, exchangeProfiles, false, replyProfiles);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("While processing UpdateAttributes message, region has local scope: {}",
                  adviseePath);
            }
          }
        }
      } catch (PRLocallyDestroyedException fre) {
        if (logger.isDebugEnabled()) {
          logger.debug("<Region Locally destroyed> /// {}", this);
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<region destroyed> /// {}", this);
        }
      }
    }

    @Override
    public void cleanUp() {
      if (filterProfile != null) {
        filterProfile.cleanUp();
      }
    }

    /**
     * Attempts to process this message with the specified <code>CacheDistributionAdvisee</code>.
     *
     * @param cda the CacheDistributionAdvisee to apply this profile to
     * @param isRealRegion true if CacheDistributionAdvisee is a real region
     */
    private void handleCacheDistributionAdvisee(CacheDistributionAdvisee cda, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, boolean isRealRegion,
        final List<Profile> replyProfiles) {
      if (cda != null) {
        handleDistributionAdvisee(cda, removeProfile, isRealRegion && exchangeProfiles,
            replyProfiles);
        if (logger.isDebugEnabled()) {
          logger.debug("While processing UpdateAttributes message, handled advisee: {}", cda);
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("While processing UpdateAttributes message, region not found: {}",
              adviseePath);
        }
      }
    }

    @Override
    public int getDSFID() {
      return CACHE_PROFILE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(getIntInfo());
      if (persistentID != null) {
        InternalDataSerializer.invokeToData(persistentID, out);
      }
      if (!gatewaySenderIds.isEmpty()) {
        writeSet(gatewaySenderIds, out);
      }
      if (!asyncEventQueueIds.isEmpty()) {
        writeSet(asyncEventQueueIds, out);
      }
      DataSerializer.writeObject(filterProfile, out);
      if (!cacheServiceProfiles.isEmpty()) {
        DataSerializer.writeObject(cacheServiceProfiles, out);
      }
    }

    private void writeSet(Set<?> set, DataOutput out) throws IOException {
      // to fix bug 47205 always serialize the Set as a HashSet.
      out.writeByte(DSCODE.HASH_SET.toByte());
      InternalDataSerializer.writeSet(set, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      int bits = in.readInt();
      setIntInfo(bits);
      if (hasPersistentID(bits)) {
        persistentID = new PersistentMemberID();
        InternalDataSerializer.invokeFromData(persistentID, in);
      }
      if (hasGatewaySenderIds(bits)) {
        gatewaySenderIds = DataSerializer.readObject(in);
      }
      if (hasAsyncEventQueueIds(bits)) {
        asyncEventQueueIds = DataSerializer.readObject(in);
      }
      filterProfile = DataSerializer.readObject(in);
      if (hasCacheServiceProfiles(bits)) {
        cacheServiceProfiles = DataSerializer.readObject(in);
      }
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("CacheProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; dataPolicy=").append(dataPolicy);
      sb.append("; hasCacheLoader=").append(hasCacheLoader);
      sb.append("; hasCacheWriter=").append(hasCacheWriter);
      sb.append("; hasCacheListener=").append(hasCacheListener);
      sb.append("; hasCacheServer=").append(hasCacheServer);
      sb.append("; scope=").append(scope);
      sb.append("; regionInitialized=").append(regionInitialized);
      sb.append("; inRecovery=").append(inRecovery);
      sb.append("; subcription=").append(subscriptionAttributes);
      sb.append("; isPartitioned=").append(isPartitioned);
      sb.append("; isGatewayEnabled=").append(isGatewayEnabled);
      sb.append("; isPersistent=").append(isPersistent);
      sb.append("; persistentID=").append(persistentID);
      if (filterProfile != null) {
        sb.append("; ").append(filterProfile);
      }
      sb.append("; gatewaySenderIds =").append(gatewaySenderIds);
      sb.append("; asyncEventQueueIds =").append(asyncEventQueueIds);
      sb.append("; IsOffHeap=").append(isOffHeap);
      sb.append("; cacheServiceProfiles=").append(cacheServiceProfiles);
    }
  }

  /** Recipient information used for getInitialImage operation */
  public static class InitialImageAdvice {
    public Set<InternalDistributedMember> getOthers() {
      return others;
    }

    public void setOthers(Set<InternalDistributedMember> others) {
      this.others = others;
    }

    public Set<InternalDistributedMember> getReplicates() {
      return replicates;
    }

    public Set<InternalDistributedMember> getNonPersistent() {
      return nonPersistent;
    }

    public Set<InternalDistributedMember> getPreloaded() {
      return preloaded;
    }

    public Set<InternalDistributedMember> getEmpties() {
      return empties;
    }

    public Set<InternalDistributedMember> getUninitialized() {
      return uninitialized;
    }

    /** Set of replicate recipients */
    protected final Set<InternalDistributedMember> replicates;

    /** Set of peers that are preloaded */
    protected final Set<InternalDistributedMember> preloaded;

    /**
     * Set of tertiary recipients which are not replicates, in which case they should all be queried
     * and a superset taken of their images. To be used only if the image cannot be obtained from
     * the replicates set.
     */
    protected Set<InternalDistributedMember> others;

    /** Set of members that might be data feeds and have EMPTY data policy */
    protected final Set<InternalDistributedMember> empties;

    /** Set of members that may not have finished initializing their caches */
    protected final Set<InternalDistributedMember> uninitialized;

    /** Set of members that are replicates but not persistent */
    final Set<InternalDistributedMember> nonPersistent;

    private final Map<InternalDistributedMember, CacheProfile> memberProfiles;


    public InitialImageAdvice(Set<InternalDistributedMember> replicates,
        Set<InternalDistributedMember> others, Set<InternalDistributedMember> preloaded,
        Set<InternalDistributedMember> empties, Set<InternalDistributedMember> uninitialized,
        Set<InternalDistributedMember> nonPersistent,
        Map<InternalDistributedMember, CacheProfile> memberProfiles) {
      this.replicates = replicates;
      this.others = others;
      this.preloaded = preloaded;
      this.empties = empties;
      this.uninitialized = uninitialized;
      this.nonPersistent = nonPersistent;
      this.memberProfiles = memberProfiles;
    }

    public InitialImageAdvice() {
      this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
          Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
          Collections.emptyMap());
    }

    @Override
    public String toString() {
      return "InitialImageAdvice(" + "replicates=" + replicates + "; others=" + others
          + "; preloaded=" + preloaded + "; empty=" + empties + "; initializing="
          + uninitialized + ")";
    }

  }

  /**
   * Provide all the replicates including persistent replicates.
   *
   * @return the set of replicate's memberIds
   * @since GemFire 5.8
   */
  public Set<InternalDistributedMember> adviseReplicates() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.dataPolicy.withReplication();
    });
  }

  /**
   * Provide only the preloadeds given a set of existing memberIds
   *
   * @return the set of preloaded's memberIds
   * @since GemFire prPersistSprint1
   */
  public Set advisePreloadeds() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.dataPolicy.withPreloaded();
    });
  }

  /**
   * Provide only the empty's (having DataPolicy.EMPTY) given a set of existing memberIds
   *
   * @return the set of replicate's memberIds
   * @since GemFire 5.8
   */
  Set adviseEmptys() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.dataPolicy.isEmpty();
    });
  }

  @Override
  protected void profileRemoved(Profile profile) {
    if (logger.isDebugEnabled()) {
      logger.debug("CDA: removing profile {}", profile);
    }
    if (getAdvisee() instanceof LocalRegion && profile != null) {
      ((LocalRegion) getAdvisee()).removeCriticalMember(profile.getDistributedMember());
    }
  }

  /**
   * Returns the list of all persistent members. For most cases, adviseInitializedPersistentMembers
   * is more appropriate. These method includes members that are still in the process of GII.
   */
  public Map<InternalDistributedMember, PersistentMemberID> advisePersistentMembers() {
    initializationGate();

    Map<InternalDistributedMember, PersistentMemberID> result =
        new HashMap<>();
    Profile[] snapshot = profiles;
    for (Profile profile : snapshot) {
      CacheProfile cp = (CacheProfile) profile;
      if (cp.persistentID != null) {
        result.put(cp.getDistributedMember(), cp.persistentID);
      }
    }

    return result;
  }

  public Map<InternalDistributedMember, PersistentMemberID> adviseInitializedPersistentMembers() {
    initializationGate();

    Map<InternalDistributedMember, PersistentMemberID> result =
        new HashMap<>();
    Profile[] snapshot = profiles;
    for (Profile profile : snapshot) {
      CacheProfile cp = (CacheProfile) profile;
      if (cp.persistentID != null && cp.persistenceInitialized) {
        result.put(cp.getDistributedMember(), cp.persistentID);
      }
    }

    return result;
  }

  Set adviseCacheServers() {
    getAdvisee().getCancelCriterion().checkCancelInProgress(null);
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return cp.hasCacheServer;
    });
  }

  // Overrided for bucket regions. This listener also receives events
  // about PR joins and leaves.
  public void addMembershipAndProxyListener(MembershipListener listener) {
    addMembershipListener(listener);
  }

  public void removeMembershipAndProxyListener(MembershipListener listener) {
    removeMembershipListener(listener);
  }

  @Override
  public boolean removeId(ProfileId memberId, boolean crashed, boolean destroyed,
      boolean fromMembershipListener) {

    boolean isPersistent = false;
    DiskStoreID persistentId = null;
    CacheDistributionAdvisee advisee = (CacheDistributionAdvisee) getAdvisee();
    if (advisee.getAttributes().getDataPolicy().withPersistence()) {
      isPersistent = true;
      CacheProfile profile = (CacheProfile) getProfile(memberId);
      if (profile != null && profile.persistentID != null) {
        persistentId = ((CacheProfile) getProfile(memberId)).persistentID.getDiskStoreId();
      }
    }

    boolean result = super.removeId(memberId, crashed, destroyed, fromMembershipListener);

    // bug #48962 - record members that leave during GII so IIOp knows about them
    if (advisee instanceof DistributedRegion) {
      DistributedRegion r = (DistributedRegion) advisee;
      if (!r.isInitialized() && !r.isUsedForPartitionedRegionBucket()) {
        if (logger.isDebugEnabled()) {
          logger.debug("recording that {} has left during initialization of {}", memberId,
              r.getName());
        }
        ImageState state = r.getImageState();
        if (isPersistent) {
          if (persistentId != null) {
            state.addLeftMember(persistentId);
          }
        } else {
          state.addLeftMember((InternalDistributedMember) memberId);
        }
      }
    }
    return result;
  }
}
