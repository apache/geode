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

import static org.apache.geode.internal.cache.CacheServerImpl.CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.locator.SerializationHelper;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LockNotHeldException;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DistributedMemberLock;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.BucketProfileUpdateMessage;
import org.apache.geode.internal.cache.partitioned.DeposePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.DeposePrimaryBucketMessage.DeposePrimaryBucketResponse;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Specialized {@link CacheDistributionAdvisor} for {@link BucketRegion BucketRegions}. The
 * <code>BucketAdvisor</code> is owned by a {@link ProxyBucketRegion} and may outlive a
 * <code>BucketRegion</code>.
 */
@SuppressWarnings("synthetic-access")
public class BucketAdvisor extends CacheDistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  private static final boolean ENFORCE_SAFE_CLOSE = false;
  // TODO: Boolean.getBoolean("gemfire.BucketAdvisor.debug.enforceSafeClose");

  /** Reference to the InternalDistributedMember that is primary. */
  private final AtomicReference<InternalDistributedMember> primaryMember = new AtomicReference<>();

  /**
   * Advice requests for {@link #adviseProfileUpdate()} delegate to the partitioned region's
   * <code>RegionAdvisor</code> to include members with {@link ProxyBucketRegion}s as well as real
   * {@link BucketRegion}s.
   */
  private final RegionAdvisor regionAdvisor;

  private final BucketRedundancyTracker redundancyTracker;

  /**
   * The bucket primary will be holding this distributed lock. Protected by synchronized(this).
   */
  private DistributedMemberLock primaryLock;

  // private static final byte MASK_HOSTING = 1; // 0001
  // private static final byte MASK_VOLUNTEERING = 2; // 0010
  // private static final byte MASK_OTHER_PRIMARY = 4; // 0100
  // private static final byte MASK_IS_PRIMARY = 8; // 1000

  private static final byte NO_PRIMARY_NOT_HOSTING = 0; // 0000_0000
  private static final byte NO_PRIMARY_HOSTING = 1; // 0000_0001
  private static final byte OTHER_PRIMARY_NOT_HOSTING = 4; // 0000_0100
  private static final byte OTHER_PRIMARY_HOSTING = 5; // 0000_0101
  private static final byte VOLUNTEERING_HOSTING = 3; // 0000_0011
  private static final byte BECOMING_HOSTING = 15; // 0000_1111
  private static final byte IS_PRIMARY_HOSTING = 9; // 0000_1001
  private static final byte CLOSED = 16; // 0001_0000

  /**
   * The current state of this BucketAdvisor which tracks which member is primary and whether or not
   * this member is hosting a real Bucket.
   */
  private volatile byte primaryState = NO_PRIMARY_NOT_HOSTING;

  /**
   * This delegate handles all volunteering for primary status. Lazily created. Protected by
   * synchronization(this).
   */
  private VolunteeringDelegate volunteeringDelegate;

  /**
   * A random number generator
   *
   * @see #getPreferredNode()
   */
  @Immutable
  private static final Random myRand = new Random();

  /**
   * A read/write lock to prevent making this bucket not primary while a write is in progress on the
   * bucket.
   */
  private final ReadWriteLock primaryMoveReadWriteLock = new ReentrantReadWriteLock();
  private final Lock primaryMoveReadLock = primaryMoveReadWriteLock.readLock();
  private final Lock primaryMoveWriteLock = primaryMoveReadWriteLock.writeLock();

  /**
   * The advisor for the bucket region that we are colocated with, if this region is a colocated
   * region.
   */
  private BucketAdvisor parentAdvisor;

  /**
   * The member that is responsible for choosing the primary for this bucket. While this field is
   * set and this member exists, this bucket won't try to become primary.
   */
  private volatile InternalDistributedMember primaryElector;

  private volatile BucketProfile localProfile;

  private volatile boolean everHadPrimary = false;

  private BucketAdvisor startingBucketAdvisor;

  private PartitionedRegion pRegion;

  private volatile boolean shadowBucketDestroyed;

  /**
   * Constructs a new BucketAdvisor for the Bucket owned by RegionAdvisor.
   *
   * @param bucket the bucket to provide metadata and advice for
   * @param regionAdvisor advisor for the PartitionedRegion
   */
  private BucketAdvisor(Bucket bucket, RegionAdvisor regionAdvisor) {
    super(bucket);
    this.regionAdvisor = regionAdvisor;
    pRegion = this.regionAdvisor.getPartitionedRegion();
    redundancyTracker =
        new BucketRedundancyTracker(pRegion.getRedundantCopies(), pRegion.getRedundancyTracker());
    resetParentAdvisor(bucket.getId());
  }

  public static BucketAdvisor createBucketAdvisor(Bucket bucket, RegionAdvisor regionAdvisor) {
    BucketAdvisor advisor = new BucketAdvisor(bucket, regionAdvisor);
    advisor.initialize();
    return advisor;
  }

  private void resetParentAdvisor(int bucketId) {
    PartitionedRegion colocatedRegion = ColocationHelper.getColocatedRegion(pRegion);
    if (colocatedRegion != null) {
      if (colocatedRegion.isFixedPartitionedRegion()) {
        List<FixedPartitionAttributesImpl> fpas = colocatedRegion.getFixedPartitionAttributesImpl();
        if (fpas != null) {
          for (FixedPartitionAttributesImpl fpa : fpas) {
            if (fpa.hasBucket(bucketId)) {
              parentAdvisor =
                  colocatedRegion.getRegionAdvisor().getBucketAdvisor(fpa.getStartingBucketID());
              break;
            }
          }
        }
      } else {
        parentAdvisor = colocatedRegion.getRegionAdvisor().getBucketAdvisor(bucketId);
      }
    } else {
      parentAdvisor = null;
    }
  }

  private void assignStartingBucketAdvisorIfFixedPartitioned() {
    if (startingBucketAdvisor != null) {
      // already assigned
      return;
    }
    if (pRegion.isFixedPartitionedRegion()) {
      List<FixedPartitionAttributesImpl> fpas = pRegion.getFixedPartitionAttributesImpl();
      if (fpas != null) {
        int bucketId = getBucket().getId();
        for (FixedPartitionAttributesImpl fpa : fpas) {
          if (fpa.hasBucket(bucketId) && bucketId != fpa.getStartingBucketID()) {
            startingBucketAdvisor = regionAdvisor.getBucketAdvisor(fpa.getStartingBucketID());
            break;
          }
        }
      }
    }
  }

  /**
   * Returns the lock that prevents the primary from moving while active writes are in progress.
   * This should be locked before checking if the local bucket is primary.
   *
   * @return the lock for in-progress write operations
   */
  public Lock getPrimaryMoveReadLock() {
    return primaryMoveReadLock;
  }

  /**
   * Returns the lock that prevents the parent's primary from moving while active writes are in
   * progress. This should be locked before checking if the local bucket is primary.
   *
   * @return the lock for in-progress write operations
   */
  Lock getParentPrimaryMoveReadLock() {
    if (parentAdvisor != null) {
      return parentAdvisor.getPrimaryMoveReadLock();
    }
    return null;
  }

  /**
   * Try to lock the primary bucket to make sure no operation is on-going at current bucket.
   *
   */
  void tryLockIfPrimary() {
    if (isPrimary()) {
      try {
        primaryMoveWriteLock.lock();
      } finally {
        primaryMoveWriteLock.unlock();
      }
    }
  }

  /**
   * Makes this <code>BucketAdvisor</code> give up being a primary and become a secondary. Does
   * nothing if not currently the primary.
   *
   * @return true if this advisor has been deposed as primary
   */
  public boolean deposePrimary() {
    if (isPrimary()) {
      primaryMoveWriteLock.lock();
      boolean needToSendProfileUpdate = false;
      try {
        removePrimary(getDistributionManager().getId());
        synchronized (this) {
          if (!isPrimary()) {
            // releasePrimaryLock();
            needToSendProfileUpdate = true;
            return true;
          } else {
            return false; // failed for some reason
          }
        }
      } finally {
        primaryMoveWriteLock.unlock();
        if (needToSendProfileUpdate) {
          sendProfileUpdate();
        }
      }
    } else {
      sendProfileUpdate();
      return true;
    }
  }

  /**
   * This calls deposePrimary on every colocated child that is directly colocated to this bucket's
   * PR. Those each in turn do the same to their child buckets and so on before returning. Each
   * depose will send a dlock release message to the grantor, wait for reply, and then also send a
   * profile update.
   * <p>
   * Caller must synchronize on this BucketAdvisor.
   *
   */
  private void deposePrimaryForColocatedChildren() {
    boolean deposedChildPrimaries = true;
    List<PartitionedRegion> colocatedChildPRs = ColocationHelper.getColocatedChildRegions(pRegion);
    if (colocatedChildPRs != null) {
      for (PartitionedRegion pr : colocatedChildPRs) {
        Bucket b = pr.getRegionAdvisor().getBucket(getBucket().getId());
        if (b != null) {
          BucketAdvisor ba = b.getBucketAdvisor();
          deposedChildPrimaries = ba.deposePrimary() && deposedChildPrimaries;
          if (b instanceof BucketRegionQueue) {
            BucketRegionQueue brq = (BucketRegionQueue) b;
            brq.decQueueSize(brq.size());
            brq.incSecondaryQueueSize(brq.size());
          }
        }
      }
    }
  }

  private void deposeOtherPrimaryBucketForFixedPartition() {
    boolean deposedOtherPrimaries = true;
    int bucketId = getBucket().getId();
    List<FixedPartitionAttributesImpl> fpas = pRegion.getFixedPartitionAttributesImpl();
    if (fpas != null) {
      for (FixedPartitionAttributesImpl fpa : fpas) {
        if (fpa.getStartingBucketID() == bucketId) {
          for (int i = (bucketId + 1); i <= fpa.getLastBucketID(); i++) {
            Bucket b = regionAdvisor.getBucket(i);
            if (b != null) {
              BucketAdvisor ba = b.getBucketAdvisor();
              deposedOtherPrimaries = ba.deposePrimary() && deposedOtherPrimaries;
            }
          }
        }
      }
    }
  }

  void removeBucket() {
    setHosting(false);
  }

  /**
   * Return (and possibly choose) a thread-sticky member from whose data store this bucket's values
   * should be read
   *
   * @return member to use for reads, null if none available
   */
  public InternalDistributedMember getPreferredNode() {

    if (isHosting()) {
      getPartitionedRegionStats().incPreferredReadLocal();
      return getDistributionManager().getId();
    }

    Profile[] locProfiles = profiles; // volatile read
    if (locProfiles.length == 0) {
      return null;
    }

    Profile selectedProfile = selectNotInitializingProfile(locProfiles);

    if (selectedProfile == null) {
      return null;
    }

    getPartitionedRegionStats().incPreferredReadRemote();
    return selectedProfile.peerMemberId;
  }

  /**
   * Get the random Profile that is not initializing BucketProfile.
   *
   */
  private Profile selectNotInitializingProfile(Profile[] inProfiles) {
    int index = 0;
    int offset = 0;
    if (inProfiles.length > 1) {
      // Pick random offset.
      offset = myRand.nextInt(inProfiles.length);
    }

    for (int i = 0; i < inProfiles.length; i++) {
      index = (offset + i) % inProfiles.length;
      BucketProfile bp = (BucketProfile) inProfiles[index];
      if (!bp.isInitializing) {
        return inProfiles[index];
      }
    }
    return null;
  }

  /**
   * Returns the thread-safe queue of primary volunteering tasks for the parent Partitioned Region.
   *
   * @return the queue of primary volunteering tasks
   */
  private Queue<Runnable> getVolunteeringQueue() {
    return regionAdvisor.getVolunteeringQueue();
  }

  /**
   * Returns the semaphore which controls the number of threads allowed to consume from the
   * {@link #getVolunteeringQueue volunteering queue}.
   *
   * @return the semaphore which controls the number of volunteering threads
   */
  private Semaphore getVolunteeringSemaphore() {
    return regionAdvisor.getVolunteeringSemaphore();
  }

  /**
   * Returns the PartitionedRegionStats.
   *
   * @return the PartitionedRegionStats
   */
  private PartitionedRegionStats getPartitionedRegionStats() {
    return regionAdvisor.getPartitionedRegionStats();
  }

  /**
   * Concurrency: protected by synchronizing on *this*
   */
  @Override
  protected void profileCreated(Profile profile) {
    regionAdvisor.incrementBucketCount(profile);
    super.profileCreated(profile);
    if (updateRedundancy() > 0) {
      // wake up any threads in waitForRedundancy or waitForPrimary
      notifyAll();
    }
    regionAdvisor.updateBucketStatus(getBucket().getId(), profile.peerMemberId, false);
    if (logger.isDebugEnabled()) {
      logger.debug("Profile added {} Profile : {}", getBucket().getFullPath(), profile);
    }
    synchronized (this) {
      updateServerBucketProfile();
    }
  }

  /**
   * Concurrency: protected by synchronizing on *this*
   */
  @Override
  protected void profileUpdated(Profile profile) {
    super.profileUpdated(profile);
    if (updateRedundancy() > 0) {
      // wake up any threads in waitForRedundancy or waitForPrimary
      notifyAll();
    }
    regionAdvisor.updateBucketStatus(getBucket().getId(), profile.peerMemberId, false);

    if (logger.isDebugEnabled()) {
      logger.debug("Profile updated {} Profile : {}", getBucket().getFullPath(), profile);
    }
    synchronized (this) {
      updateServerBucketProfile();
    }
  }

  /**
   * Concurrency: protected by synchronizing on *this*
   */
  @Override
  protected void profileRemoved(Profile profile) {
    if (profile != null) {
      regionAdvisor.updateBucketStatus(getBucket().getId(),
          profile.getDistributedMember(), true);
      regionAdvisor.decrementsBucketCount(profile);
    }
    updateRedundancy();

    if (logger.isDebugEnabled()) {
      logger.debug("Profile removed {} the member lost {} Profile : {}", getBucket().getFullPath(),
          profile != null ? profile.getDistributedMember() : null, profile);
    }
    synchronized (this) {
      updateServerBucketProfile();
    }
  }

  @Override
  public boolean shouldSyncForCrashedMember(InternalDistributedMember id) {
    BucketProfile profile = (BucketProfile) getProfile(id);
    return (profile != null) && (profile.isPrimary);
  }

  @Override
  public DistributedRegion getRegionForDeltaGII() {
    DistributedRegion result = super.getRegionForDeltaGII();
    if (result == null && getAdvisee() instanceof ProxyBucketRegion) {
      result = ((ProxyBucketRegion) getAdvisee()).getHostedBucketRegion();
    }
    return result;
  }



  /**
   * Called by the RegionAdvisor.profileRemoved, this method tests to see if the missing member is
   * the primary elector for this bucket.
   *
   * We can't call this method from BucketAdvisor.profileRemoved, because the primaryElector may not
   * actually host the bucket.
   *
   */
  public void checkForLostPrimaryElector(Profile profile) {
    // If the member that went away was in the middle of creating
    // the bucket, finish the bucket creation.
    ProfileId elector = primaryElector;
    if (elector != null && elector.equals(profile.getDistributedMember())) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Bucket {} lost the member responsible for electing the primary. Finishing bucket creation",
            getBucket().getFullPath());
      }
      primaryElector = getBucket().getDistributionManager().getId();
      getBucket().getDistributionManager().getExecutors().getWaitingThreadPool().execute(
          () -> getBucket().getPartitionedRegion().getRedundancyProvider()
              .finishIncompleteBucketCreation(getBucket().getId()));
    }
  }

  /**
   * Only allows profiles that actually hosting this bucket. If the profile is primary, then
   * primaryMember will be set to that member but only if we are not already the primary.
   *
   * @param profile the profile to add (must be a BucketProfile)
   * @param forceProfile true will force profile to be added even if member is not in distributed
   *        view
   *
   * @see #adviseProfileUpdate()
   */
  @Override
  public boolean putProfile(Profile profile, boolean forceProfile) {
    assert profile instanceof BucketProfile;
    BucketProfile bp = (BucketProfile) profile;

    // Only hosting buckets will be initializing, the isInitializing boolean is to
    // allow for early entry into the advisor for GII purposes
    if (!bp.isHosting && !bp.isInitializing) {
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "BucketAdvisor#putProfile early out");
      }
      return false; // Do not allow introduction of proxy profiles, they don't provide anything
                    // useful
      // isHosting = false, isInitializing = false
    }
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "BucketAdvisor#putProfile profile=<{}> force={}; profile = {}", profile, forceProfile,
          bp);
    }
    // isHosting = false, isInitializing = true
    // isHosting = true, isInitializing = false
    // isHosting = true, isInitializing = true... (false state)

    final boolean applied;
    synchronized (this) {
      // force new membership version in the advisor so that the
      // state flush mechanism can capture any updates to the bucket
      // MIN_VALUE is intended as a somewhat unique value for potential debug purposes
      profile.initialMembershipVersion = Long.MIN_VALUE;
      applied = super.putProfile(profile, forceProfile);
      // skip following block if isPrimary to avoid race where we process late
      // arriving OTHER_PRIMARY profile after we've already become primary
      if (applied && !isPrimary()) {
        if (bp.isPrimary) {
          setPrimaryMember(bp.getDistributedMember());
        } else {
          notPrimary(bp.getDistributedMember());
        }
      } // if: !isPrimary

    } // synchronized
    return applied;
  }

  private static <E> Set<E> newSetFromMap(Map<E, Boolean> map) {
    if (map.isEmpty()) {
      return new SetFromMap<>(map);
    }
    throw new IllegalArgumentException();
  }

  @SuppressWarnings("NullableProblems")
  private static class SetFromMap<E> extends AbstractSet<E> implements Serializable {
    private static final long serialVersionUID = 2454657854757543876L;

    // must named as it, to pass serialization compatibility test.
    private Map<E, Boolean> m;

    private transient Set<E> backingSet;

    SetFromMap(final Map<E, Boolean> map) {
      super();
      m = map;
      backingSet = map.keySet();
    }

    @Override
    public boolean equals(Object object) {
      return backingSet.equals(object);
    }

    @Override
    public int hashCode() {
      return backingSet.hashCode();
    }

    @Override
    public boolean add(E object) {
      return m.put(object, Boolean.TRUE) == null;
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public String toString() {
      return backingSet.toString();
    }

    @Override
    public boolean contains(Object object) {
      return backingSet.contains(object);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
      return backingSet.containsAll(collection);
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public boolean remove(Object object) {
      return m.remove(object) != null;
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
      return backingSet.retainAll(collection);
    }

    @Override
    public Object[] toArray() {
      return backingSet.toArray();
    }

    @Override
    public <T> T[] toArray(T[] contents) {
      return backingSet.toArray(contents);
    }

    @Override
    public Iterator<E> iterator() {
      return backingSet.iterator();
    }

    @Override
    public int size() {
      return m.size();
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      backingSet = m == null ? Collections.emptySet() : m.keySet();
    }
  }

  /**
   * repopulates the RegionAdvisor's location information for this bucket
   */
  private void updateServerBucketProfile() {
    int bucketId = getBucket().getId();
    Set<ServerBucketProfile> serverProfiles =
        newSetFromMap(new HashMap<>());
    for (Profile p : profiles) {
      if (p instanceof ServerBucketProfile) {
        serverProfiles.add((ServerBucketProfile) p);
      }
    }
    regionAdvisor.setClientBucketProfiles(bucketId, serverProfiles);
  }

  /**
   * Only for local profile.
   *
   */
  public synchronized void updateServerBucketProfile(BucketProfile p) {
    localProfile = p;
  }

  public BucketProfile getLocalProfile() {
    return localProfile;
  }

  @Override
  public boolean removeId(ProfileId memberId, boolean crashed, boolean destroyed,
      boolean fromMembershipListener) {
    boolean hadBucketRegion = super.removeId(memberId, crashed, destroyed, fromMembershipListener);
    if (hadBucketRegion) {
      // do NOT call notPrimary under synchronization
      try {
        notPrimary((InternalDistributedMember) memberId);
      } catch (CancelException e) {
        // must be closing the cache - no need to try to become primary
      }
    }
    return hadBucketRegion;
  }

  /**
   * Removes the profile for the specified member. If that profile is marked as primary, this will
   * call {@link #notPrimary(InternalDistributedMember)}.
   *
   * @param memberId the member to remove the profile for
   * @param serialNum specific serial number to remove
   * @return true if a matching profile for the member was found
   */
  @Override
  public boolean removeIdWithSerial(InternalDistributedMember memberId, int serialNum,
      boolean regionDestroyed) {
    boolean hadBucketRegion = super.removeIdWithSerial(memberId, serialNum, regionDestroyed);
    if (hadBucketRegion) {
      // do NOT call notPrimary under synchronization
      notPrimary(memberId);
    }
    forceNewMembershipVersion();
    return hadBucketRegion;
  }

  @Override
  public Set<InternalDistributedMember> adviseProfileExchange() {
    // delegate up to RegionAdvisor to include members that might have
    // ProxyBucketRegion without a real BucketRegion
    Assert.assertTrue(regionAdvisor.isInitialized());
    return regionAdvisor.adviseBucketProfileExchange();
  }

  @Override
  public Set<InternalDistributedMember> adviseProfileUpdate() {
    // delegate up to RegionAdvisor to include members that might have
    // ProxyBucketRegion without a real BucketRegion
    return regionAdvisor.adviseGeneric();
  }

  /**
   * Sets hosting to false and returns without closing. Calling closeAdvisor will actually close
   * this advisor.
   */
  @Override
  public void close() {
    // ok, so BucketRegion extends DistributedRegion which calls close on the
    // advisor, BUT the ProxyBucketRegion truly owns the advisor and only it
    // should be able to close the advisor...
    //
    // if a BucketRegion closes, it will call this method and we want to change
    // our state to NOT_HOSTING
    //
    // see this.closeAdvisor()
    setHosting(false);
  }

  /**
   * Blocks until there is a known primary and return that member, but only if there are real bucket
   * regions that exist. If there are no real bucket regions within the distribution config's
   * member-timeout setting * 3 (time required to eject a member) + 15000, then this returns null.
   *
   * kbanks: reworked this method to avoid JIT issue #40639
   *
   * @return the member who is primary for this bucket
   */
  public InternalDistributedMember getPrimary() {
    InternalDistributedMember primary = getExistingPrimary();
    if (primary == null) {
      primary = waitForNewPrimary();
    }
    return primary;
  }

  /**
   * This method was split out from getPrimary() due to bug #40639 and is only intended to be called
   * from within that method.
   *
   * @see #getPrimary()
   * @return the existing primary (if it is still in the view) otherwise null
   */
  private InternalDistributedMember getExistingPrimary() {
    return basicGetPrimaryMember();
  }

  /**
   * This method was split out from getPrimary() due to bug #40639 and is only intended to be called
   * from within that method.
   *
   * @see #getPrimary()
   * @return the new primary
   */
  private InternalDistributedMember waitForNewPrimary() {
    DistributionManager dm = regionAdvisor.getDistributionManager();
    DistributionConfig config = dm.getConfig();
    // failure detection period
    long timeout = config.getMemberTimeout() * 3L;
    // plus time for a new member to become primary
    timeout += Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "BucketAdvisor.getPrimaryTimeout",
        15000L);
    return waitForPrimaryMember(timeout);
  }

  /**
   * Marks member as not primary. Initiates volunteerForPrimary if this member is hosting a real
   * bucket. This method does nothing if the member parameter is the current member.
   *
   * @param member the member who is not primary
   */
  public void notPrimary(InternalDistributedMember member) {
    // Fix for 43569. Only the deposePrimary call should
    // make the local member drop the primary lock.
    if (!member.equals(getDistributionManager().getId())) {
      removePrimary(member);
    }
  }

  /**
   * Marks member as not primary. Initiates volunteerForPrimary if this member is hosting a real
   * bucket.
   *
   * @param member the member who is not primary
   */
  private void removePrimary(InternalDistributedMember member) {
    boolean needToVolunteerForPrimary = false;
    if (!isClosed()) { // hole: requestPrimaryState not hosting
      initializationGate();
    }
    boolean lostPrimary = false;
    try {
      synchronized (this) {
        boolean wasPrimary = isPrimary() && getDistributionManager().getId().equals(member);
        final InternalDistributedMember currentPrimary = primaryMember.get();
        if (currentPrimary != null && currentPrimary.equals(member)) {
          if (logger.isDebugEnabled()) {
            logger.debug("[BucketAdvisor.notPrimary] {} for {}", member, this);
          }
          primaryMember.set(null);
        } else {
          return;
        }

        if (isClosed()) {
          // possibly closed if caller comes from outside this advisor
          return; // return quietly
        }
        // member is primary... need to change state to NO_PRIMARY_xxx
        if (isHosting()) {
          requestPrimaryState(NO_PRIMARY_HOSTING);
          if (pRegion.isFixedPartitionedRegion()) {
            InternalDistributedMember primaryMember =
                regionAdvisor.adviseFixedPrimaryPartitionDataStore(getBucket().getId());
            needToVolunteerForPrimary = primaryMember == null || primaryMember.equals(member);
          } else {
            needToVolunteerForPrimary = true;
          }
        } else {
          requestPrimaryState(NO_PRIMARY_NOT_HOSTING);
        }
        if (wasPrimary) {
          lostPrimary = true;
        }

        // try to fix yet another cause of bug 36881
        findAndSetPrimaryMember();
      }
    } finally {
      if (lostPrimary) {
        invokeAfterSecondaryInPartitionListeners();
        Bucket br = regionAdvisor.getBucket(getBucket().getId());
        if (br instanceof BucketRegion) {
          ((BucketRegion) br).beforeReleasingPrimaryLockDuringDemotion();
        }

        releasePrimaryLock();
        // this was a deposePrimary call so we need to depose children as well
        deposePrimaryForColocatedChildren();
        if (pRegion.isFixedPartitionedRegion()) {
          deposeOtherPrimaryBucketForFixedPartition();
        }
      }
    }

    if (needToVolunteerForPrimary) {
      volunteerForPrimary();
    }
  }

  /**
   * Returns the ProxyBucketRegion which owns this advisor.
   *
   * @return the ProxyBucketRegion which owns this advisor
   */
  public ProxyBucketRegion getProxyBucketRegion() {
    return (ProxyBucketRegion) getAdvisee();
  }

  /**
   * Actually close this advisor for real. Called by ProxyBucketRegion only. Calling this method
   * actually closes this advisor whereas {@link #close()} only sets hosting to false.
   */
  void closeAdvisor() {
    boolean wasPrimary;
    synchronized (this) {
      if (isClosed()) {
        return;
      }
      wasPrimary = isPrimary();
      super.close();
      requestPrimaryState(CLOSED);
      redundancyTracker.closeBucket();
      localProfile = null;
    }
    if (wasPrimary) {
      releasePrimaryLock();
    }
  }

  /**
   * Returns true if this advisor has been closed.
   *
   * @return true if this advisor has been closed
   */
  protected boolean isClosed() {
    return primaryState == CLOSED;
  }

  /**
   * Returns true if this member is currently marked as primary.
   *
   * @return true if this member is currently marked as primary
   */
  public boolean isPrimary() {
    return primaryState == IS_PRIMARY_HOSTING;
  }

  /**
   * Returns true if this member is currently volunteering for primary.
   *
   * @return true if this member is currently volunteering for primary
   */
  private boolean isVolunteering() {
    return primaryState == VOLUNTEERING_HOSTING;
  }

  /**
   * Returns true if this member is currently attempting to become primary.
   *
   * @return true if this member is currently attempting to become primary
   */
  private boolean isBecomingPrimary() {
    synchronized (this) {
      return primaryState == BECOMING_HOSTING && volunteeringDelegate != null
          && volunteeringDelegate.isAggressive();
    }
  }

  /**
   * Returns true if this member is currently hosting real bucket.
   *
   * @return true if this member is currently hosting real bucket
   */
  public boolean isHosting() {
    final byte primaryState = this.primaryState;
    return primaryState == NO_PRIMARY_HOSTING || primaryState == OTHER_PRIMARY_HOSTING
        || primaryState == VOLUNTEERING_HOSTING || primaryState == BECOMING_HOSTING
        || primaryState == IS_PRIMARY_HOSTING;
  }

  /**
   * Attempt to acquire lock for primary until a primary exists. Caller hands off responsibility to
   * an executor (waiting pool) and returns early.
   */
  public void volunteerForPrimary() {
    InternalDistributedMember elector = primaryElector;
    if (elector != null && regionAdvisor.hasPartitionedRegion(elector)) {
      // another server will determine the primary node
      return;
    }

    primaryElector = null;

    initializationGate();

    synchronized (this) {
      if (isVolunteering() || isClosed() || !isHosting()) {
        // only one thread should be attempting to volunteer at one time
        return;
      }

      if (volunteeringDelegate == null) {
        setVolunteeringDelegate(new VolunteeringDelegate());
      }
      volunteeringDelegate.volunteerForPrimary();

    }
  }

  protected void setVolunteeringDelegate(VolunteeringDelegate delegate) {
    volunteeringDelegate = delegate;
  }

  /**
   * Makes this <code>BucketAdvisor</code> become the primary if it is already a secondary.
   *
   * @param isRebalance true if directed to become primary by rebalancing
   * @return true if this advisor succeeds in becoming the primary
   */
  public boolean becomePrimary(boolean isRebalance) {
    initializationGate();

    long startTime = getPartitionedRegionStats().startPrimaryTransfer(isRebalance);
    try {
      long waitTime = 2000; // time each iteration will wait
      while (!isPrimary()) {
        getAdvisee().getCancelCriterion().checkCancelInProgress(null);
        boolean attemptToBecomePrimary = false;
        boolean attemptToDeposePrimary = false;

        if (Thread.currentThread().isInterrupted()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Breaking from becomePrimary loop due to thread interrupt flag being set");
          }
          break;
        }
        if (isClosed() || !isHosting()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Breaking from becomePrimary loop because {} is closed or not hosting",
                this);
          }
          break;
        }

        VolunteeringDelegate vDelegate = null;
        synchronized (this) {
          if (isVolunteering()) {
            // standard volunteering attempt already in progress...
            if (logger.isDebugEnabled()) {
              logger.debug("Waiting for volunteering thread {}. Time left: {} ms", this, waitTime);
            }
            wait(waitTime); // spurious wakeup ok
            continue;

          } else if (isBecomingPrimary()) {
            // reattempt to depose otherPrimary...
            attemptToDeposePrimary = true;

          } else {
            // invoke becomePrimary AFTER sync is released in this thread...
            vDelegate = volunteeringDelegate;
            if (vDelegate == null) {
              vDelegate = new VolunteeringDelegate();
              volunteeringDelegate = vDelegate;
            }
          } // else
        } // synchronized

        if (vDelegate != null) {
          // Use the snapshot 'vDelegate' instead of 'this.volunteeringDelegate' since we are not
          // synced here.
          attemptToBecomePrimary = vDelegate.reserveForBecomePrimary(); // no sync!
        }

        // release synchronization and then call becomePrimary
        if (attemptToBecomePrimary) {
          synchronized (this) {
            if (volunteeringDelegate == null) {
              volunteeringDelegate = new VolunteeringDelegate();
            }
            volunteeringDelegate.volunteerForPrimary();
            attemptToDeposePrimary = true;
          } // synchronized
          Thread.sleep(10);
        } // attemptToBecomePrimary

        // RACE: slight race condition with thread that's actually requesting the lock

        if (attemptToDeposePrimary) {
          InternalDistributedMember otherPrimary = getPrimary();
          if (otherPrimary != null && !getDistributionManager().getId().equals(otherPrimary)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Attempting to depose primary on {} for {}", otherPrimary, this);
            }
            DeposePrimaryBucketResponse response =
                DeposePrimaryBucketMessage.send(otherPrimary, pRegion, getBucket().getId());
            if (response != null) {
              response.waitForRepliesUninterruptibly();
              if (logger.isDebugEnabled()) {
                logger.debug("Deposed primary on {}", otherPrimary);
              }
            }
          }
          Thread.sleep(10);
        } // attemptToDeposePrimary

      } // while

    } catch (InterruptedException e) {
      // abort and return null
      Thread.currentThread().interrupt();

    } finally {
      getPartitionedRegionStats().endPrimaryTransfer(startTime, isPrimary(), isRebalance);
    }

    return isPrimary();
  }

  /**
   * Check the primary member shortcut. Does not query the advisor. Should only be used when the
   * advisor should not be consulted directly.
   *
   * @return the member or null if no primary exists
   */
  public InternalDistributedMember basicGetPrimaryMember() {
    return primaryMember.get();
  }

  /**
   * Invoked when the primary lock has been acquired by this VM.
   *
   * @return true if successfully changed state to IS_PRIMARY
   */
  private boolean acquiredPrimaryLock() {
    if (logger.isDebugEnabled()) {
      logger.debug("Acquired primary lock for BucketID {} PR : {}", getBucket().getId(),
          regionAdvisor.getPartitionedRegion().getFullPath());
    }
    boolean changedStateToIsPrimary = false;
    // Hold the primary move lock until we send a
    // profile update. This will prevent writes
    // from occurring until all members know that
    // this member is now the primary.
    boolean shouldInvokeListeners = false;
    primaryMoveWriteLock.lock();
    try {
      synchronized (this) {
        if (isHosting() && (isVolunteering() || isBecomingPrimary())) {
          Bucket br = regionAdvisor.getBucket(getBucket().getId());
          if (br instanceof BucketRegion) {
            ((BucketRegion) br).beforeAcquiringPrimaryState();
          }
          if (requestPrimaryState(IS_PRIMARY_HOSTING)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Acquired primary lock for setting primary now BucketID {} PR : {}",
                  getBucket().getId(), regionAdvisor.getPartitionedRegion().getFullPath());
            }
            setPrimaryMember(getDistributionManager().getId());
            changedStateToIsPrimary = true;
            if (hasPrimary() && isPrimary()) {
              shouldInvokeListeners = true;
            }
          }
        }
      }

      if (shouldInvokeListeners) {
        invokePartitionListeners();
      }
      return changedStateToIsPrimary;
    } finally {
      try {
        if (changedStateToIsPrimary) {
          // send profile update AFTER releasing sync
          sendProfileUpdate();

          Bucket br = regionAdvisor.getBucket(getBucket().getId());
          if (br instanceof BucketRegion) {
            ((BucketRegion) br).processPendingSecondaryExpires();
          }
          if (br instanceof BucketRegionQueue) { // Shouldn't it be AbstractBucketRegionQueue
            BucketRegionQueue brq = (BucketRegionQueue) br;
            brq.incQueueSize(brq.size());
            brq.decSecondaryQueueSize(brq.size());
          }
          if (br instanceof BucketRegion) {
            ((BucketRegion) br).afterAcquiringPrimaryState();
          }
        } else {
          // release primary lock AFTER releasing sync
          releasePrimaryLock();
        }
      } finally {
        primaryMoveWriteLock.unlock();
      }
    }
  }

  private void invokePartitionListeners() {
    PartitionListener[] listeners = pRegion.getPartitionListeners();
    if (listeners == null || listeners.length == 0) {
      return;
    }
    for (PartitionListener listener : listeners) {
      if (listener != null) {
        listener.afterPrimary(getBucket().getId());
      }
    }
  }

  private void invokeAfterSecondaryInPartitionListeners() {
    PartitionListener[] listeners = pRegion.getPartitionListeners();
    if (listeners == null || listeners.length == 0) {
      return;
    }
    for (PartitionListener listener : listeners) {
      if (listener != null) {
        listener.afterSecondary(getBucket().getId());
      }
    }
  }

  /**
   * Lazily gets the lock for acquiring primary lock. Caller must handle null. If DLS, Cache, or
   * DistributedSystem are shutting down then null will be returned. If DLS does not yet exist and
   * createDLS is false then null will be returned.
   *
   * @param createDLS true will create DLS if it does not exist
   * @return distributed lock indicating primary member or null
   */
  private DistributedMemberLock getPrimaryLock(boolean createDLS) {
    synchronized (this) {
      if (primaryLock == null) {
        DistributedLockService dls = DistributedLockService
            .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
        if (dls == null) {
          if (!createDLS || getProxyBucketRegion().getCache().isClosed()) {
            return null; // cache closure has destroyed the DLS
          }
          try { // TODO: call GemFireCache#getPartitionedRegionLockService
            dls = DLockService.create(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME,
                getAdvisee().getSystem(), true /* distributed */, true /* destroyOnDisconnect */,
                true /* automateFreeResources */);
          } catch (IllegalArgumentException e) {
            // indicates that the DLS is already created
            dls = DistributedLockService
                .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
            if (dls == null) {
              // another thread destroyed DLS after this thread called create
              return null; // ok, caller will loop if necessary
            }
          }
          // TODO: we need a good NotConnectedException to replace
          // IllegalStateException and ShutdownException
          // perhaps: DistributedSystemUnavailableException
          catch (IllegalStateException | DistributedSystemDisconnectedException e) {
            // create still throws IllegalStateException if isDisconnecting is true
            return null;
          } // this would certainly prevent us from creating a DLS... messy

        }
        primaryLock = new DistributedMemberLock(dls, getAdvisee().getName(),
            DistributedMemberLock.NON_EXPIRING_LEASE,
            DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY);
      }
      return primaryLock;
    }
  }

  private void acquirePrimaryRecursivelyForColocated() {
    final List<PartitionedRegion> colocatedWithList =
        ColocationHelper.getColocatedChildRegions(regionAdvisor.getPartitionedRegion());
    if (colocatedWithList != null) {
      for (PartitionedRegion childPR : colocatedWithList) {
        Bucket b = childPR.getRegionAdvisor().getBucket(getBucket().getId());
        BucketAdvisor childBA = b.getBucketAdvisor();
        Assert.assertHoldsLock(childBA, false);
        boolean acquireForChild = false;

        if (logger.isDebugEnabled()) {
          logger.debug(
              "BucketAdvisor.acquirePrimaryRecursivelyForColocated: about to take lock for bucket: {} of PR: {} with isHosting={}",
              getBucket().getId(), childPR.getFullPath(), childBA.isHosting());
        }
        childBA.primaryMoveWriteLock.lock();
        try {
          if (childBA.isHosting()) {
            if (isPrimary()) {
              if (!childBA.isPrimary()) {
                childBA.setVolunteering();
                boolean acquired = childBA.acquiredPrimaryLock();
                acquireForChild = true;
                if (acquired && pRegion.isFixedPartitionedRegion()) {
                  childBA.acquirePrimaryForRestOfTheBucket();
                }
              } else {
                acquireForChild = true;
              }
            }
          } // if isHosting
          if (acquireForChild) {
            childBA.acquirePrimaryRecursivelyForColocated();
          }
        } finally {
          childBA.primaryMoveWriteLock.unlock();
        }
      }
    }
  }

  private void acquirePrimaryForRestOfTheBucket() {
    List<FixedPartitionAttributesImpl> fpas = pRegion.getFixedPartitionAttributesImpl();
    if (fpas != null) {
      int bucketId = getBucket().getId();
      for (FixedPartitionAttributesImpl fpa : fpas) {
        if (fpa.getStartingBucketID() == bucketId) {
          for (int i = bucketId + 1; i <= fpa.getLastBucketID();) {
            Bucket b = regionAdvisor.getBucket(i++);
            if (b != null) {
              BucketAdvisor ba = b.getBucketAdvisor();
              ba.primaryMoveWriteLock.lock();
              try {
                if (ba.isHosting()) {
                  if (!ba.isPrimary()) {
                    ba.setVolunteering();
                    ba.acquiredPrimaryLock();
                  }
                }
              } finally {
                ba.primaryMoveWriteLock.unlock();
              }
            }
          }
        }
      }
    }
  }

  /**
   * Sets volunteering to true. Returns true if the state of volunteering was changed. Returns false
   * if voluntering was already equal to true. Caller should do nothing if false is returned.
   */
  private boolean setVolunteering() {
    synchronized (this) {
      return requestPrimaryState(VOLUNTEERING_HOSTING);
    }
  }

  /**
   * Sets becoming primary to true. Returns true if the state of becoming was changed. Returns false
   * if becoming was already equal to true. Caller should do nothing if false is returned.
   */
  private boolean setBecoming() {
    synchronized (this) {
      return requestPrimaryState(BECOMING_HOSTING);
    }
  }

  /**
   * Wait briefly for a primary member to be identified.
   *
   * @param timeout time in milliseconds to wait for a primary
   * @return the primary bucket host
   */
  private InternalDistributedMember waitForPrimaryMember(long timeout) {
    synchronized (this) {
      // let's park this thread and wait for a primary!
      StopWatch timer = new StopWatch(true);
      long warnTime = getDistributionManager().getConfig().getAckWaitThreshold() * 1000L;
      boolean loggedWarning = false;
      try {
        for (;;) {
          // bail out if the system starts closing
          getAdvisee().getCancelCriterion().checkCancelInProgress(null);
          final InternalCache cache = getBucket().getCache();
          if (cache != null && cache.isCacheAtShutdownAll()) {
            throw cache.getCacheClosedException("Cache is shutting down");
          }

          if (getBucketRedundancy() == -1) {
            // there are no real buckets in other vms... no reason to wait
            return null;
          }
          getProxyBucketRegion().getPartitionedRegion().checkReadiness();
          if (isClosed()) {
            break;
          }
          long elapsed = timer.elapsedTimeMillis();
          long timeLeft = timeout - elapsed;
          if (timeLeft <= 0) {
            break;
          }
          if (getBucketRedundancy() == -1 || isClosed()) {
            break; // early out... all bucket regions are gone or we closed
          }
          InternalDistributedMember primary = basicGetPrimaryMember();
          if (primary != null) {
            return primary;
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for bucket {}. Time left :{} ms", this, timeLeft);
          }

          // Log a warning if we have waited for the ack wait threshold time.
          if (!loggedWarning) {
            long timeUntilWarning = warnTime - elapsed;
            if (timeUntilWarning <= 0) {
              logger
                  .warn(
                      "{} secs have elapsed waiting for a primary for bucket {}. Current bucket owners {}",
                      new Object[] {warnTime / 1000L, this, adviseInitialized()});
              // log a warning;
              loggedWarning = true;
            } else {
              timeLeft = timeLeft > timeUntilWarning ? timeUntilWarning : timeLeft;
            }
          }
          wait(timeLeft); // spurious wakeup ok
        }
      } catch (InterruptedException e) {
        // abort and return null
        Thread.currentThread().interrupt();
      } finally {
        if (loggedWarning) {
          logger.info(
              "Wait for primary completed");
        }
      }
      return null;
    }
  }

  /**
   * How long to wait, in millisecs, for redundant buckets to exist
   */
  private static final long BUCKET_REDUNDANCY_WAIT = 15000L; // 15 seconds

  /**
   * Wait the desired redundancy to be met.
   *
   * @param minRedundancy the amount of desired redundancy.
   * @return true if desired redundancy is detected
   */
  public boolean waitForRedundancy(int minRedundancy) {
    synchronized (this) {
      // let's park this thread and wait for redundancy!
      StopWatch timer = new StopWatch(true);
      try {
        for (;;) {
          if (getBucketRedundancy() >= minRedundancy) {
            return true;
          }
          getProxyBucketRegion().getPartitionedRegion().checkReadiness();
          if (isClosed()) {
            return false;
          }
          long timeLeft = BUCKET_REDUNDANCY_WAIT - timer.elapsedTimeMillis();
          if (timeLeft <= 0) {
            return false;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for bucket {}", this);
          }
          wait(timeLeft); // spurious wakeup ok
        }
      } catch (InterruptedException e) {
        // abort and return null
        Thread.currentThread().interrupt();
      }
      return false;
    }
  }

  synchronized void clearPrimaryElector() {
    primaryElector = null;
  }

  synchronized void setPrimaryElector(InternalDistributedMember newPrimaryElector) {
    // Only set the new primary elector if we have not yet seen
    // a primary for this bucket.
    if (primaryElector != null) {
      if (newPrimaryElector != null && !regionAdvisor.hasPartitionedRegion(newPrimaryElector)) {
        // no longer a participant - don't use it
        primaryElector = null;
      } else {
        primaryElector = newPrimaryElector;
      }
    }
  }


  public synchronized void initializePrimaryElector(InternalDistributedMember newPrimaryElector) {
    // For child buckets, we want the parent bucket to take care'
    // of finishing an incomplete bucket creation, so only set the elector for
    // the leader region.
    if (parentAdvisor == null) {
      if (newPrimaryElector != null && !regionAdvisor.hasPartitionedRegion(newPrimaryElector)) {
        // no longer a participant - don't use it
        primaryElector = null;
      } else {
        primaryElector = newPrimaryElector;
      }
    }
  }

  /**
   * Invoked when real bucket is created for hosting in this VM.
   *
   * @param value true to begin hosting; false to end hosting
   */
  protected void setHosting(boolean value) {
    // boolean needToNotPrimarySelf = false;
    boolean needToVolunteerForPrimary = false;
    boolean wasPrimary;
    synchronized (this) {
      wasPrimary = isPrimary();
      if (isClosed()) {
        return;
      }
      if (value) { // setting to HOSTING...
        if (hasPrimary()) {
          requestPrimaryState(OTHER_PRIMARY_HOSTING);
        } else {
          requestPrimaryState(NO_PRIMARY_HOSTING);
          needToVolunteerForPrimary = true;
        }
      }

      else { // setting to NOT_HOSTING...
        if (hasPrimary()) { // has primary...
          if (isPrimary()) {
            requestPrimaryState(NO_PRIMARY_NOT_HOSTING);
            primaryMember.set(null);
            findAndSetPrimaryMember();
          } else {
            requestPrimaryState(OTHER_PRIMARY_NOT_HOSTING);
          }
        } else { // no primary...
          // acquiredPrimaryLock will check isHosting and release if not hosting
          requestPrimaryState(NO_PRIMARY_NOT_HOSTING);
        }
      }
      volunteeringDelegate = null;

      // Note - checkRedundancy has the side effect that it updates the stats.
      // We need to invoke checkRedundancy here, regardless of whether we
      // need this notify.
      if (updateRedundancy() > 0 && isHosting()) {
        // wake up any threads in waitForRedundancy or waitForPrimary
        notifyAll();
      }
    }
    if (wasPrimary) {
      releasePrimaryLock();
    }

    if (logger.isTraceEnabled()) {
      logger.trace("setHosting: {} needToVolunteerForPrimary={} primaryElector: {}", this,
          needToVolunteerForPrimary, primaryElector);
    }
    /*
     * if (needToNotPrimarySelf) { notPrimary(getAdvisee().getDistributionManager().getId()); }
     */
    if (needToVolunteerForPrimary) {
      volunteerForPrimary();
    }

    sendProfileUpdate();
  }

  /**
   * Sends updated profile for this member to every member with the <code>PartitionedRegion</code>.
   * <p>
   * Never call this method while synchronized on this BucketAdvisor. This will result in
   * distributed deadlocks.
   */
  private void sendProfileUpdate() {
    if (getDistributionManager().getSystem().isLoner()) {
      // no one to send the profile update... return to prevent bug 39760
      return;
    }
    // make sure caller is not synchronized or we'll deadlock
    Assert.assertTrue(!Thread.holdsLock(this),
        "Attempting to sendProfileUpdate while synchronized may result in deadlock");
    // NOTE: if this assert fails, you COULD use the WaitingThreadPool in DM

    final int partitionedRegionId = pRegion.getPRId();
    final int bucketId = ((ProxyBucketRegion) getAdvisee()).getBucketId();

    BucketProfile bp = (BucketProfile) createProfile();
    updateServerBucketProfile(bp);
    InternalDistributedMember primary = basicGetPrimaryMember();
    HashSet<InternalDistributedMember> hostsAndProxyMembers = new HashSet<>();
    if (primary != null && !primary.equals(getDistributionManager().getId())) {
      hostsAndProxyMembers.add(primary); // Add the primary
    }
    hostsAndProxyMembers.addAll(adviseGeneric()); // Add all members hosting the bucket
    hostsAndProxyMembers.addAll(adviseProfileUpdate()); // Add all proxy instances that could use
                                                        // the bucket
    ReplyProcessor21 reply = BucketProfileUpdateMessage.send(hostsAndProxyMembers,
        getDistributionManager(), partitionedRegionId, bucketId, bp, true);
    if (reply != null) {
      reply.waitForRepliesUninterruptibly();
    }
  }

  /**
   * Returns true if the a primary is known.
   */
  private boolean hasPrimary() {
    final byte primaryState = this.primaryState;
    return primaryState == OTHER_PRIMARY_NOT_HOSTING || primaryState == OTHER_PRIMARY_HOSTING
        || primaryState == IS_PRIMARY_HOSTING;
  }

  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    if (!pRegion.isShadowPR()) {
      Set<BucketServerLocation66> serverLocations = getBucketServerLocations(version);
      if (!serverLocations.isEmpty()) {
        return new ServerBucketProfile(memberId, version, getBucket(), (HashSet) serverLocations);
      }
    }
    return new BucketProfile(memberId, version, getBucket());
  }

  protected Set<BucketServerLocation66> getBucketServerLocations(int version) {
    InternalCache cache = getProxyBucketRegion().getCache();
    List<CacheServer> servers = cache.getCacheServers();
    if (servers.isEmpty()) {
      return Collections.emptySet();
    }
    HashSet<BucketServerLocation66> serverLocations = new HashSet<>();
    for (CacheServer cacheServer : servers) {
      CacheServerImpl server = (CacheServerImpl) cacheServer;
      try {
        String serverExternalAddress;
        if (server.isRunning() && ((serverExternalAddress = server.getExternalAddress()) != null)) {
          BucketServerLocation66 location = new BucketServerLocation66(getBucket().getId(),
              server.getPort(), serverExternalAddress, getBucket().isPrimary(),
              Integer.valueOf(version).byteValue(), server.getCombinedGroups());
          serverLocations.add(location);
        }
      } catch (IllegalStateException e) {
        if (!(e.getMessage() != null
            && e.getMessage().equals(CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE))) {
          throw e;
        }
      }
    }
    return serverLocations;
  }

  /**
   * Sets primaryMember and notifies all. Caller must be synced on this.
   *
   * @param id the member to use as primary for this bucket
   */
  private void setPrimaryMember(InternalDistributedMember id) {
    if (!getDistributionManager().getId().equals(id)) {
      // volunteerForPrimary handles primary state change if its our id
      if (isHosting()) {
        requestPrimaryState(OTHER_PRIMARY_HOSTING);
      } else {
        requestPrimaryState(OTHER_PRIMARY_NOT_HOSTING);
      }
    }
    primaryMember.set(id);
    everHadPrimary = true;

    if (id != null && id.equals(primaryElector)) {
      primaryElector = null;
    }
    notifyAll(); // wake up any threads in waitForPrimaryMember
  }

  void setHadPrimary() {
    everHadPrimary = true;
  }

  boolean getHadPrimary() {
    return everHadPrimary;
  }

  public InternalDistributedMember getPrimaryElector() {
    return primaryElector;
  }

  /**
   * Get the current number of bucket hosts and update the redundancy statistics for the region
   *
   * @return number of current bucket hosts
   */
  private int updateRedundancy() {
    int numBucketHosts = getNumInitializedBuckets();
    if (!isClosed()) {
      redundancyTracker.updateStatistics(numBucketHosts);
    }
    return numBucketHosts;
  }

  /**
   * Returns all {@link InternalDistributedMember}s currently flagged as primary.
   * <p>
   * Since profile messages may arrive out of order from different members, more than one member may
   * temporarily be flagged as primary.
   * <p>
   * The user of this BucketAdvisor should simply assume that the first profile is primary until the
   * dust settles, leaving only one primary profile.
   *
   * @return zero or greater array of primary members
   */
  private InternalDistributedMember[] findPrimaryMembers() {
    Set<InternalDistributedMember> primaryMembers = adviseFilter(profile -> {
      assert profile instanceof BucketProfile;
      BucketProfile srp = (BucketProfile) profile;
      return srp.isPrimary;
    });
    if (primaryMembers.size() > 1 && logger.isDebugEnabled()) {
      logger.debug("[findPrimaryProfiles] found the following primary members for {}: {}",
          getAdvisee().getName(), primaryMembers);
    }
    return primaryMembers.toArray(new InternalDistributedMember[0]);
  }

  /**
   * Searches through profiles to find first profile that is flagged as primary and sets
   * {@link #primaryMember} to it. Caller must synchronize on this BucketAdvisor.
   *
   * @return true if a primary member was found and used
   * @see #findAndSetPrimaryMember()
   */
  private boolean findAndSetPrimaryMember() {
    if (isPrimary()) {
      setPrimaryMember(getDistributionManager().getDistributionManagerId());
      return true;
    }
    InternalDistributedMember[] primaryMembers = findPrimaryMembers();
    if (primaryMembers.length > 0) {
      setPrimaryMember(primaryMembers[0]);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the current redundancy of the this bucket, including the locally hosted bucket if it
   * exists.
   *
   * @return current number of hosts of this bucket ; -1 if there are no hosts
   */
  public int getBucketRedundancy() {
    return redundancyTracker.getCurrentRedundancy();
  }

  Set<InternalDistributedMember> adviseInitialized() {
    return adviseFilter(profile -> {
      assert profile instanceof BucketProfile;
      BucketProfile bucketProfile = (BucketProfile) profile;
      return bucketProfile.isHosting;
    });

  }

  Set<InternalDistributedMember> adviseRecoveredFromDisk() {
    return regionAdvisor.adviseInitializedDataStore();
  }

  /**
   * Get the number of members that are hosting the bucket, and have finished initialization.
   *
   * This method is currently only used to check the bucket redundancy just before creating the
   * bucket. If it is used more frequently, it might be better to cache this count.
   */
  private int getNumInitializedBuckets() {
    Profile[] locProfiles = profiles; // grab current profiles
    int count = 0;
    for (Profile profile : locProfiles) {
      BucketProfile bucketProfile = (BucketProfile) profile;
      if (bucketProfile.isHosting) {
        count++;
      }
    }
    if (isHosting()) {
      count++;
    }
    return count;
  }

  private Bucket getBucket() {
    return (Bucket) getAdvisee();
  }

  /**
   * Releases the primary lock for this bucket.
   */
  private void releasePrimaryLock() {
    // We don't have a lock if we have a parent advisor
    if (parentAdvisor != null) {
      return;
    }
    assignStartingBucketAdvisorIfFixedPartitioned();
    if (startingBucketAdvisor != null) {
      return;
    }
    try {
      DistributedMemberLock thePrimaryLock = getPrimaryLock(false);
      if (thePrimaryLock != null) {
        thePrimaryLock.unlock();
      }
    } catch (LockNotHeldException e) {
      Assert.assertTrue(!isHosting(), "Got LockNotHeldException for Bucket = " + this);
    } catch (LockServiceDestroyedException e) {
      Assert.assertTrue(isClosed(),
          "BucketAdvisor was not closed before destroying PR lock service");
    }
  }

  private String primaryStateToString() {
    return primaryStateToString(primaryState);
  }

  /**
   * Returns string representation of the primary state value.
   *
   * @param value the primary state to return string for
   * @return string representation of primaryState
   */
  private String primaryStateToString(byte value) {
    switch (value) {
      case NO_PRIMARY_NOT_HOSTING:
        return "NO_PRIMARY_NOT_HOSTING";
      case NO_PRIMARY_HOSTING:
        return "NO_PRIMARY_HOSTING";
      case OTHER_PRIMARY_NOT_HOSTING:
        return "OTHER_PRIMARY_NOT_HOSTING";
      case OTHER_PRIMARY_HOSTING:
        return "OTHER_PRIMARY_HOSTING";
      case VOLUNTEERING_HOSTING:
        return "VOLUNTEERING_HOSTING";
      case BECOMING_HOSTING:
        return "BECOMING_HOSTING";
      case IS_PRIMARY_HOSTING:
        return "IS_PRIMARY_HOSTING";
      case CLOSED:
        return "CLOSED";
      default:
        return "<unhandled primaryState " + value + " >";
    }
  }

  /**
   * Requests change to the requested primary state. Controls all state changes pertaining to
   * primary state. Caller must be synchronized on this.
   *
   * @param requestedState primaryState to change to
   * @return true if the requestedState change was completed
   * @throws IllegalStateException if an illegal state change was attempted
   */
  private boolean requestPrimaryState(byte requestedState) {
    final byte fromState = primaryState;
    switch (fromState) {
      case NO_PRIMARY_NOT_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_NOT_HOSTING:
            // race condition ok, return false
            return false;
          case NO_PRIMARY_HOSTING:
            primaryState = requestedState;
            break;
          case OTHER_PRIMARY_NOT_HOSTING:
            primaryState = requestedState;
            break;
          case OTHER_PRIMARY_HOSTING:
            primaryState = requestedState;
            break;
          case BECOMING_HOSTING:
            // race condition during close is ok, return false
            return false;
          case VOLUNTEERING_HOSTING:
            // race condition during close is ok, return false
            return false;
          case CLOSED:
            primaryState = requestedState;
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(), primaryStateToString(requestedState)));
        }
        break;
      case NO_PRIMARY_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_NOT_HOSTING:
            primaryState = requestedState;
            break;
          // case OTHER_PRIMARY_NOT_HOSTING: -- enable for bucket migration
          // this.primaryState = requestedState;
          // break;
          case NO_PRIMARY_HOSTING:
            // race condition ok, return false
            return false;
          case VOLUNTEERING_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.putStartTime(this, stats.startVolunteering());
          }
            break;
          case BECOMING_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.putStartTime(this, stats.startVolunteering());
          }
            break;
          case OTHER_PRIMARY_HOSTING:
            primaryState = requestedState;
            break;
          case CLOSED:
            primaryState = requestedState;
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(),
                primaryStateToString(requestedState)));
        }
        break;
      case OTHER_PRIMARY_NOT_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_NOT_HOSTING:
            primaryState = requestedState;
            break;
          case OTHER_PRIMARY_NOT_HOSTING:
            // race condition ok, return false
            return false;
          case OTHER_PRIMARY_HOSTING:
            primaryState = requestedState;
            break;
          case BECOMING_HOSTING:
            // race condition during close is ok, return false
            return false;
          case VOLUNTEERING_HOSTING:
            // race condition during close is ok, return false
            return false;
          case CLOSED:
            primaryState = requestedState;
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(),
                primaryStateToString(requestedState)));
        }
        break;
      case OTHER_PRIMARY_HOSTING:
        switch (requestedState) {
          // case NO_PRIMARY_NOT_HOSTING: -- enable for bucket migration
          // this.primaryState = requestedState;
          // break;
          case OTHER_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called
            primaryState = requestedState;
            break;
          case OTHER_PRIMARY_HOSTING:
            // race condition ok, return false
            return false;
          case NO_PRIMARY_HOSTING:
            primaryState = requestedState;
            break;
          case CLOSED:
            primaryState = requestedState;
            break;
          case VOLUNTEERING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case BECOMING_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.putStartTime(this, stats.startVolunteering());
          }
            break;
          case IS_PRIMARY_HOSTING:
            // race condition ok, probably race in HA where other becomes
            // primary and immediately leaves while we have try-lock message
            // enroute to grantor
            primaryState = requestedState;
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(),
                primaryStateToString(requestedState)));
        }
        break;
      case VOLUNTEERING_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          case OTHER_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called
            // Profile update for other primary may have slipped in
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          case NO_PRIMARY_HOSTING:
            // race condition occurred, return false and stay in volunteering
            return false;
          case IS_PRIMARY_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.incPrimaryBucketCount(1);
            stats.endVolunteeringBecamePrimary(stats.removeStartTime(this));
          }
            break;
          case OTHER_PRIMARY_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringOtherPrimary(stats.removeStartTime(this));
          }
            break;
          case VOLUNTEERING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case BECOMING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case CLOSED:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(),
                primaryStateToString(requestedState)));
        }
        break;
      case BECOMING_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          case OTHER_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called
            // Profile update for other primary may have slipped in
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          case NO_PRIMARY_HOSTING:
            // race condition occurred, return false and stay in volunteering
            return false;
          case IS_PRIMARY_HOSTING:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.incPrimaryBucketCount(1);
            stats.endVolunteeringBecamePrimary(stats.removeStartTime(this));
          }
            break;
          case OTHER_PRIMARY_HOSTING:
            return false;
          case VOLUNTEERING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case BECOMING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case CLOSED:
            primaryState = requestedState; {
            PartitionedRegionStats stats = getPartitionedRegionStats();
            stats.endVolunteeringClosed(stats.removeStartTime(this));
          }
            break;
          default:
            throw new IllegalStateException(String.format("Cannot change from %s to %s",
                primaryStateToString(),
                primaryStateToString(requestedState)));
        }
        break;
      case IS_PRIMARY_HOSTING:
        switch (requestedState) {
          case NO_PRIMARY_HOSTING:
            // rebalancing must have moved the primary
            changeFromPrimaryTo(requestedState);
            break;
          // case OTHER_PRIMARY_HOSTING: -- enable for bucket migration
          // // rebalancing must have moved the primary
          // changeFromPrimaryTo(requestedState);
          // break;
          case OTHER_PRIMARY_NOT_HOSTING:
            // rebalancing must have moved the primary and primary
            changeFromPrimaryTo(requestedState);
            break;
          case NO_PRIMARY_NOT_HOSTING:
            // May occur when setHosting(false) is called due to closing
            changeFromPrimaryTo(requestedState);
            break;
          case VOLUNTEERING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case BECOMING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case CLOSED:
            changeFromPrimaryTo(requestedState);
            break;
          default:
            throw new IllegalStateException("Cannot change from " + primaryStateToString()
                + " to " + primaryStateToString(requestedState));
        }
        break;
      case CLOSED:
        switch (requestedState) {
          case CLOSED:
            Exception e = new Exception(
                "Attempted to close BucketAdvisor that is already CLOSED");
            logger.warn("Attempted to close BucketAdvisor that is already CLOSED",
                e);
            break;
          case VOLUNTEERING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case BECOMING_HOSTING:
            // race condition ok, return false to abort volunteering
            return false;
          case IS_PRIMARY_HOSTING:
            // Commonly occurs when closing and volunteering thread is still running
            return false;
          case OTHER_PRIMARY_NOT_HOSTING:
            // Commonly occurs when a putProfile occurs during closure
            return false;
          default:
            throw new IllegalStateException(
                String.format("Cannot change from %s to %s for bucket %s",
                    primaryStateToString(),
                    primaryStateToString(requestedState), getAdvisee().getName()));
        }
    }
    return primaryState == requestedState;
  }

  private void changeFromPrimaryTo(byte requestedState) {
    try {
      primaryState = requestedState;
    } finally {
      getPartitionedRegionStats().incPrimaryBucketCount(-1);
    }
  }

  @Override
  public Set adviseDestroyRegion() {
    // fix for bug 37604 - tell all owners of the pr that the bucket is being
    // destroyed. This is needed when bucket cleanup is performed
    return regionAdvisor.adviseAllPRNodes();
  }

  /**
   * returns the set of all the members in the system which require both DistributedCacheOperation
   * messages and notification-only partition messages
   *
   * @return a set of recipients requiring both cache-op and notification messages
   * @since GemFire 5.7
   */
  public Set<InternalDistributedMember> adviseRequiresTwoMessages() {
    return adviseNotInitialized();
  }


  private Set<InternalDistributedMember> adviseNotInitialized() {
    return adviseFilter(profile -> {
      assert profile instanceof CacheProfile;
      CacheProfile cp = (CacheProfile) profile;
      return !cp.regionInitialized;
    });
  }


  @Override
  public Set adviseNetWrite() {
    return regionAdvisor.adviseNetWrite();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("[BucketAdvisor ").append(getAdvisee().getFullPath())
        .append(':').append(getAdvisee().getSerialNumber()).append(": ");
    sb.append("state=").append(primaryStateToString());
    sb.append("]");
    return sb.toString();
  }

  // A listener for events on this bucket, and also for the entire PR
  @Override
  public void addMembershipAndProxyListener(MembershipListener listener) {
    super.addMembershipAndProxyListener(listener);
    regionAdvisor.addMembershipListener(listener);
  }

  @Override
  public void removeMembershipAndProxyListener(MembershipListener listener) {
    regionAdvisor.removeMembershipListener(listener);
    super.removeMembershipAndProxyListener(listener);
  }

  /**
   * Called from endBucket creation. We send out a profile to notify others that the persistence is
   * initialized.
   */
  void endBucketCreation() {
    sendProfileUpdate();
  }

  /**
   * Profile information for a remote bucket counterpart.
   */
  public static class BucketProfile extends CacheProfile {
    /** True if this profile's member is attempting to initialize the bucket */
    public boolean isInitializing;
    /** True if this profile's member is the primary for this bucket */
    public boolean isPrimary;
    /**
     * True if the profile is coming from a real BucketRegion acceptible states hosting = false,
     * init = true hosting = true, init = false hosting = false, init = false unacceptible states
     * hosting = true, init = true
     */
    public boolean isHosting;


    /**
     * True if the bucket has been removed from this host. (
     */
    public boolean removed;

    public BucketProfile() {}

    public BucketProfile(InternalDistributedMember memberId, int version, Bucket bucket) {
      super(memberId, version);
      isPrimary = bucket.isPrimary();
      isHosting = bucket.isHosting();
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("BucketAdvisor.BucketProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; isPrimary=").append(isPrimary);
      sb.append("; isHosting=").append(isHosting);
      sb.append("; isInitializing=").append(isInitializing);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      isPrimary = in.readBoolean();
      isHosting = in.readBoolean();
      isInitializing = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(isPrimary);
      out.writeBoolean(isHosting);
      out.writeBoolean(isInitializing);
    }

    @Override
    public int getDSFID() {
      return BUCKET_PROFILE;
    }
  }
  /**
   * Profile information for a remote bucket hosted by cache servers.
   */
  public static class ServerBucketProfile extends BucketProfile {

    private Set<BucketServerLocation66> bucketServerLocations;

    private int bucketId;

    public ServerBucketProfile() {}

    public ServerBucketProfile(InternalDistributedMember memberId, int version, Bucket bucket,
        HashSet<BucketServerLocation66> serverLocations) {
      super(memberId, version, bucket);
      bucketId = bucket.getId();
      bucketServerLocations = serverLocations;
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("BucketAdvisor.ServerBucketProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      for (BucketServerLocation66 location : bucketServerLocations) {
        sb.append("; hostName=").append(location.getHostName());
        sb.append("; port=").append(location.getPort());
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      bucketServerLocations = SerializationHelper.readBucketServerLocationSet(in);
      bucketId = DataSerializer.readPrimitiveInt(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      SerializationHelper.writeBucketServerLocationSet(bucketServerLocations, out);
      DataSerializer.writePrimitiveInt(bucketId, out);
    }

    public Set<BucketServerLocation66> getBucketServerLocations() {
      return bucketServerLocations;
    }

    @Override
    public int getDSFID() {
      return SERVER_BUCKET_PROFILE;
    }

    @Override
    public int hashCode() {
      BucketServerLocation66 sl = (BucketServerLocation66) bucketServerLocations.toArray()[0];
      return 31 * bucketId + sl.getPort();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof ServerBucketProfile))
        return false;
      final ServerBucketProfile other = (ServerBucketProfile) obj;
      if (other.bucketId != bucketId) {
        return false;
      }
      if (other.bucketServerLocations.size() != bucketServerLocations.size()) {
        return false;
      }
      return other.bucketServerLocations.containsAll(bucketServerLocations);
    }
  }

  /**
   * Handles the actual volunteering to become primary bucket. Ensures that only one thread is ever
   * volunteering at one time.
   *
   */
  class VolunteeringDelegate {
    /**
     * Reference to the Thread that is currently volunteering. Protected by
     * synchronized(volunteeringLock).
     */
    private Thread volunteeringThread;

    private boolean aggressive = false;

    /**
     * Returns true if this delegate is aggressively trying to become the primary even if another
     * member is already the primary.
     *
     * @return true if this aggressively trying to become the primary
     */
    boolean isAggressive() {
      synchronized (BucketAdvisor.this) {
        return aggressive;
      }
    }

    /**
     * Initiates volunteering for primary. Repeated calls are harmless. Invoked by the
     * BucketAdvisor. Caller must be synchronized on BucketAdvisor.
     */
    void volunteerForPrimary() {
      boolean handedOff = false;
      while (!handedOff) {
        getAdvisee().getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          execute(this::doVolunteerForPrimary);
          handedOff = true;
        } catch (InterruptedException e) {
          interrupted = true;
          getAdvisee().getCancelCriterion().checkCancelInProgress(e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    /**
     * Reserves this delegate for the current thread to call becomePrimary. Necessary because caller
     * of doVolunteerForPrimary must not be synchronized on BucketAdvisor.
     *
     * @return true if successfully reserved for becomePrimary
     */
    boolean reserveForBecomePrimary() {
      synchronized (BucketAdvisor.this) {
        if (volunteeringThread != null) {
          return false;
        }
        aggressive = true;
        return true;
      }
    }

    /**
     * Invoked by the thread that performs the actual volunteering work.
     */
    void doVolunteerForPrimary() {
      if (!beginVolunteering()) {
        return;
      }
      boolean dlsDestroyed = false;
      try {

        if (logger.isDebugEnabled()) {
          logger.debug("Begin volunteerForPrimary for {}", BucketAdvisor.this);
        }
        DistributedMemberLock thePrimaryLock = null;
        while (continueVolunteering()) {
          // Fix for 41865 - We can't send out profiles while holding the
          // sync on this advisor, because that will cause a deadlock.
          // Holding the primaryMoveWriteLock here instead prevents any
          // operations from being performed on this primary until the child regions
          // are synced up. It also prevents a depose from happening until then.
          BucketAdvisor parentBA = parentAdvisor;
          primaryMoveWriteLock.lock();
          try {
            boolean acquiredLock;
            getAdvisee().getCancelCriterion().checkCancelInProgress(null);
            // Check our parent advisor and set our state
            // accordingly
            if (parentBA != null) {
              // Fix for 44350 - we don't want to get a primary move lock on
              // the advisor, because that might deadlock with a user thread.
              // However, since all depose/elect operations on the parent bucket
              // cascade to the child bucket and get the child bucket move lock,
              // if should be safe to check this without the lock here.
              if (parentBA.isPrimary() && !isPrimary()) {
                acquiredLock = acquiredPrimaryLock();
              } else {
                return;
              }
            } else {
              // we're not colocated, need to get the dlock
              assignStartingBucketAdvisorIfFixedPartitioned();
              if (startingBucketAdvisor != null) {
                Assert.assertHoldsLock(this, false);
                synchronized (startingBucketAdvisor) {
                  if (startingBucketAdvisor.isPrimary() && !isPrimary()) {
                    acquiredLock = acquiredPrimaryLock();
                  } else {
                    return;
                  }
                }
              } else {
                if (thePrimaryLock == null) {
                  thePrimaryLock = getPrimaryLock(true);
                  if (thePrimaryLock == null) {
                    // InternalDistributedSystem.isDisconnecting probably
                    // prevented us from
                    // creating the DLS... hope there's a thread closing this
                    // advisor but
                    // it's probably not safe to assert that it already happened
                    return;
                  }
                }
                Assert.assertTrue(!thePrimaryLock.holdsLock());
                if (isAggressive()) {
                  acquiredLock = thePrimaryLock.tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                } else {
                  acquiredLock = thePrimaryLock.tryLock();
                }
                if (acquiredLock) {
                  acquiredLock = acquiredPrimaryLock();
                }
              } // parentAdvisor == null
            }
            if (acquiredLock) {
              // if the lock has been acquired then try to do the same for colocated PR's too
              // Here if somehow a bucket can't acquire a lock
              // we assume that it is in the process of becoming primary through
              // BucketAdvisor.volunteerForPrimary()
              // Either way we have to guarantee that the bucket becomes primary for sure.(How to
              // guarantee?)
              acquirePrimaryRecursivelyForColocated();
              acquirePrimaryForRestOfTheBucket();
              return;
            }
          } finally {
            primaryMoveWriteLock.unlock();
          }
          // else: acquiredPrimaryLock released thePrimaryLock

          if (!continueVolunteering()) {
            // this avoids calling the wait below...
            return;
          }

          waitIfNoPrimaryMemberFound();
        } // while
      } catch (LockServiceDestroyedException e) {
        dlsDestroyed = true;
        handleException(e, true);
      } catch (RegionDestroyedException | CancelException e) {
        handleException(e, false);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        handleException(e, false);
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("Exit volunteerForPrimary for {}; dlsDestroyed={}", BucketAdvisor.this,
              dlsDestroyed);
        }
        endVolunteering();
        // if (isPrimary()) {
        // Bucket bucket = getBucket();
        // if (bucket instanceof ProxyBucketRegion) {
        // bucket = ((ProxyBucketRegion)bucket).getHostedBucketRegion();
        // }
        // }
      }
    }

    /**
     * Called from catch blocks in {@link #doVolunteerForPrimary()}. Handles the exception properly
     * based on advisor settings and shutdown condition.
     *
     * @param e the RuntimeException that was caught while volunteering
     * @param loggit true if message should be logged if shutdown condition is not met
     */
    private void handleException(Exception e, boolean loggit) {
      boolean safe = isClosed() || getAdvisee().getCancelCriterion().isCancelInProgress();
      if (!safe) {
        if (ENFORCE_SAFE_CLOSE) {
          Assert.assertTrue(safe,
              "BucketAdvisor was not closed properly.");
        } else if (loggit) {
          logger.warn("BucketAdvisor was not closed properly.", e);
        }
      }
    }

    private boolean beginVolunteering() {
      synchronized (BucketAdvisor.this) {
        if (Thread.currentThread().equals(volunteeringThread)) {
          return true; // this thread is already volunteering or reserved
        }
        if (volunteeringThread != null) {
          // another thread is already volunteering
          return false;
        }
        volunteeringThread = Thread.currentThread();
        boolean changedState = false;
        try {
          if (isAggressive()) {
            changedState = setBecoming();
          } else {
            changedState = setVolunteering();
          }
          return changedState;
        } finally {
          if (!changedState) {
            aggressive = false;
            volunteeringThread = null;
          }
        }
      }
    }

    private boolean continueVolunteering() {
      synchronized (BucketAdvisor.this) {
        // false if caller is not the volunteeringThread
        if (!Thread.currentThread().equals(volunteeringThread)) {
          return false;
        }
        if (!isVolunteering() && !isBecomingPrimary()) {
          return false;
        }

        // false if primaryMember is not null
        if (!isAggressive() && basicGetPrimaryMember() != null) {
          return false;
        }
        // false if this member is already primary
        if (isPrimary()) {
          return false;
        }
        // false if closed
        if (isClosed()) {
          return false;
        }
        // false if no longer hosting
        return isHosting();

        // must be true... need to continue volunteering
      }
    }

    private void endVolunteering() {
      if (Thread.currentThread().equals(volunteeringThread)) {
        volunteeringThread = null;
        aggressive = false;
      }
    }

    private void waitIfNoPrimaryMemberFound() {
      synchronized (BucketAdvisor.this) {
        if (basicGetPrimaryMember() == null) {
          waitForPrimaryMember(100);
          if (basicGetPrimaryMember() == null) {
            findAndSetPrimaryMember();
          }
        }
      }
    }

    /**
     * Executes the primary volunteering task after queuing it in the
     * {@link BucketAdvisor#getVolunteeringQueue()}. A number of threads equal to
     * {@link RegionAdvisor#VOLUNTEERING_THREAD_COUNT} are permitted to consume from the queue by
     * acquiring permits from {@link BucketAdvisor#getVolunteeringSemaphore()}.
     *
     * @param volunteeringTask the task to queue and then execute in waiting thread pool
     *
     */
    private void execute(Runnable volunteeringTask) throws InterruptedException {
      // @todo: instead of having a semaphore and queue on RegionAdvisor
      // we should have an executor which limits its max threads to
      // VOLUNTEERING_THREAD_COUNT.
      if (Thread.interrupted())
        throw new InterruptedException();
      Queue<Runnable> volunteeringQueue = getVolunteeringQueue();
      synchronized (volunteeringQueue) {
        // add the volunteering task
        volunteeringQueue.add(volunteeringTask);
        if (getVolunteeringSemaphore().tryAcquire()) {
          // ensure there is a thread consuming the queue
          boolean handedOff = false;
          try {
            getDistributionManager().getExecutors().getWaitingThreadPool().execute(consumeQueue());
            handedOff = true;
          } finally {
            if (!handedOff) {
              getVolunteeringSemaphore().release();
            }
          }
        }
      }
    }

    /**
     * Returns the runnable used to consume the volunteering queue. The executing thread(s) will
     * consume from the queue until it is empty.
     *
     * @return runnable for consuming the volunteering queue
     */
    private Runnable consumeQueue() {
      return () -> {
        getPartitionedRegionStats().incVolunteeringThreads(1);
        boolean releaseSemaphore = true;
        try {
          Queue<Runnable> volunteeringQueue = getVolunteeringQueue();
          Runnable queuedWork;
          while (true) {
            // SystemFailure.checkFailure();
            getAdvisee().getCancelCriterion().checkCancelInProgress(null);
            synchronized (volunteeringQueue) {
              // synchronized volunteeringQueue for coordination between threads adding
              // work to the queue and checking for a consuming thread and the existing
              // consuming thread to determine if it can exit since the queue is empty.
              queuedWork = volunteeringQueue.poll();
              if (queuedWork == null) {
                // the queue is empty... no more work... so return
                // @todo why release the semaphore here are sync'ed?
                // we could just let the finally block do it.
                getVolunteeringSemaphore().release();
                releaseSemaphore = false;
                return;
              }
              // still more work in the queue so let's run it
            }
            try {
              queuedWork.run();
            } catch (CancelException e) {
              return;
            } catch (RuntimeException e) {
              // log and continue consuming queue
              logger.error(e.getMessage(), e);
            }
          }
        } finally {
          getPartitionedRegionStats().incVolunteeringThreads(-1);
          if (releaseSemaphore) {
            // Clean up, just in case
            getVolunteeringSemaphore().release();
          }
        }
      };
    }
  }

  void setShadowBucketDestroyed(boolean destroyed) {
    shadowBucketDestroyed = destroyed;
  }

  public boolean getShadowBucketDestroyed() {
    return shadowBucketDestroyed;
  }
}
