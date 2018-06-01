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
package org.apache.geode.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * RegionVersionVector tracks the highest region-level version number of operations applied to a
 * region for each member that has the region.
 * <p>
 *
 */
public abstract class RegionVersionVector<T extends VersionSource<?>>
    implements DataSerializableFixedID, MembershipListener {

  private static final Logger logger = LogService.getLogger();

  public static boolean DEBUG =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "VersionVector.VERBOSE"); // TODO:LOG:CONVERT:
                                                                                       // REMOVE
                                                                                       // THIS



  //////////////////// The following statics exist for unit testing. ////////////////////////////

  /** maximum ms wait time while waiting for dominance to be achieved */
  public static long MAX_DOMINANCE_WAIT_TIME =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "max-dominance-wait-time", 5000);

  /** maximum ms pause time while waiting for dominance to be achieved */
  public static long DOMINANCE_PAUSE_TIME =
      Math.min(Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "dominance-pause-time", 300),
          MAX_DOMINANCE_WAIT_TIME);

  private static int INITIAL_CAPACITY = 2;
  private static int CONCURRENCY_LEVEL = 2;
  private static float LOAD_FACTOR = 0.75f;
  ////////////////////////////////////////////////////////////////////////////////////////////////



  /** map of member to version h older. This is the actual version "vector" */
  private ConcurrentHashMap<T, RegionVersionHolder<T>> memberToVersion;

  /** current version in the local region for generating next version */
  private AtomicLong localVersion = new AtomicLong(0);
  /**
   * The list of exceptions for the local member. The version held in this RegionVersionHolder may
   * not be accurate, but the exception list is. We can have exceptions for our own id if we recover
   * from disk or GII from a peer that has exceptions from us.
   *
   * The version held in this object can lag behind the localVersion atomic long, because that long
   * is incremented without obtaining a lock. Operations that use the localException list are
   * responsible for updating the version of the local exceptions under lock.
   */
  private RegionVersionHolder<T> localExceptions;

  /** highest reaped tombstone region-version for this member */
  private AtomicLong localGCVersion = new AtomicLong(0);

  /** the member that this version vector applies to */
  private T myId;



  /**
   * a flag stating whether this vector contains only the version information for a single member.
   * This is used when a member crashed to transmit only the version information for that member.
   */
  private boolean singleMember;

  /** a flag to prevent accidental serialization of a live member */
  private transient boolean isLiveVector;

  private transient LocalRegion region;

  private ConcurrentHashMap<T, Long> memberToGCVersion;

  /** map of canonical IDs for this RVV that are not in the memberToVersion map */
  @SuppressWarnings("unchecked")
  private transient Map<T, T> canonicalIds = Collections.EMPTY_MAP;

  private final Object canonicalIdLock = new Object();

  /** is recording disabled? */
  private transient boolean recordingDisabled;

  /** is this a vector in a client cache? */
  private transient boolean clientVector;

  /**
   * this read/write lock is used to stop generation of new versions by the vector while a
   * region-level operation is underway. The locking scheme assumes that only one region-level RVV
   * operation is allowed at a time on a region across the distributed system. If that changes then
   * the locking scheme here may need additional work.
   */
  private final transient ReentrantReadWriteLock versionLock = new ReentrantReadWriteLock();
  private transient volatile boolean locked; // this is only modified by the version locking thread
  private transient volatile boolean doUnlock; // this is only modified by the version locking
                                               // thread
  private transient InternalDistributedMember lockOwner; // guarded by lockWaitSync

  private final transient Object clearLockSync = new Object(); // sync for coordinating thread
                                                               // startup and lockOwner setting

  /**
   * constructor used to create a cloned vector
   */
  protected RegionVersionVector(T ownerId, ConcurrentHashMap<T, RegionVersionHolder<T>> vector,
      long version, ConcurrentHashMap<T, Long> gcVersions, long gcVersion, boolean singleMember,
      RegionVersionHolder<T> localExceptions) {
    this.myId = ownerId;
    this.memberToVersion = vector;
    this.memberToGCVersion = gcVersions;
    this.localGCVersion.set(gcVersion);
    this.localVersion.set(version);
    this.singleMember = singleMember;
    this.localExceptions = localExceptions;
  }

  /**
   * deserialize a cloned vector
   */
  public RegionVersionVector() {
    this.memberToVersion = new ConcurrentHashMap<T, RegionVersionHolder<T>>(INITIAL_CAPACITY,
        LOAD_FACTOR, CONCURRENCY_LEVEL);
    this.memberToGCVersion =
        new ConcurrentHashMap<T, Long>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  }

  /**
   * create a live version vector for a region
   */
  public RegionVersionVector(T ownerId) {
    this(ownerId, null);
  }

  /**
   * create a live version vector for a region
   */
  public RegionVersionVector(T ownerId, LocalRegion owner) {
    this(ownerId, owner, 0);
  }

  @TestingOnly
  RegionVersionVector(T ownerId, LocalRegion owner, long version) {
    this.myId = ownerId;
    this.isLiveVector = true;
    this.region = owner;
    this.localExceptions = new RegionVersionHolder<T>(0);
    this.memberToVersion =
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
    this.memberToGCVersion =
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
    this.localVersion.set(version);
  }

  /**
   * Retrieve a vector that can be sent to another member. This clones all of the version
   * information to protect against concurrent modification during serialization
   */
  public RegionVersionVector<T> getCloneForTransmission() {
    Map<T, RegionVersionHolder<T>> liveHolders;
    liveHolders = new HashMap<T, RegionVersionHolder<T>>(this.memberToVersion);
    ConcurrentHashMap<T, RegionVersionHolder<T>> clonedHolders =
        new ConcurrentHashMap<T, RegionVersionHolder<T>>(liveHolders.size(), LOAD_FACTOR,
            CONCURRENCY_LEVEL);
    for (Map.Entry<T, RegionVersionHolder<T>> entry : liveHolders.entrySet()) {
      clonedHolders.put(entry.getKey(), entry.getValue().clone());
    }
    ConcurrentHashMap<T, Long> gcVersions = new ConcurrentHashMap<T, Long>(
        this.memberToGCVersion.size(), LOAD_FACTOR, CONCURRENCY_LEVEL);
    gcVersions.putAll(this.memberToGCVersion);
    RegionVersionHolder<T> clonedLocalHolder;
    clonedLocalHolder = this.localExceptions.clone();
    // Make sure the holder that we send to the peer does
    // have an accurate RegionVersionHolder for our local version
    return createCopy(this.myId, clonedHolders, this.localVersion.get(), gcVersions,
        this.localGCVersion.get(), false, clonedLocalHolder);
  }

  protected abstract RegionVersionVector<T> createCopy(T ownerId,
      ConcurrentHashMap<T, RegionVersionHolder<T>> vector, long version,
      ConcurrentHashMap<T, Long> gcVersions, long gcVersion, boolean singleMember,
      RegionVersionHolder<T> clonedLocalHolder);


  /**
   * Retrieve a vector that can be sent to another member. This clones only the version information
   * for the given ID.
   * <p>
   * The clone returned by this method does not have distributed garbage-collection information.
   */
  public RegionVersionVector<T> getCloneForTransmission(T mbr) {
    Map<T, RegionVersionHolder<T>> liveHolders;
    liveHolders = new HashMap<T, RegionVersionHolder<T>>(this.memberToVersion);
    RegionVersionHolder<T> holder = liveHolders.get(mbr);
    if (holder == null) {
      holder = new RegionVersionHolder<T>(-1);
    } else {
      holder = holder.clone();
    }
    return createCopy(this.myId,
        new ConcurrentHashMap<T, RegionVersionHolder<T>>(Collections.singletonMap(mbr, holder)), 0,
        new ConcurrentHashMap<T, Long>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL), 0, true,
        new RegionVersionHolder<T>(-1));
  }


  /**
   * Retrieve a collection of tombstone GC region-versions
   */
  public Map<T, Long> getTombstoneGCVector() {
    Map<T, Long> result;
    synchronized (memberToGCVersion) {
      result = new HashMap<T, Long>(this.memberToGCVersion);
    }
    if (this.localGCVersion.get() != 0) {
      result.put(this.myId, this.localGCVersion.get());
    }
    return result;
  }


  /** returns true if all of the GC versions in the given map have already been processed here */
  public boolean containsTombstoneGCVersions(Map<T, Long> regionGCVersions) {
    Long myVersion = regionGCVersions.get(this.myId);
    if (myVersion != null) {
      if (this.localGCVersion.get() < myVersion.longValue()) {
        return false;
      }
    }
    synchronized (this.memberToGCVersion) {
      for (Map.Entry<T, Long> entry : regionGCVersions.entrySet()) {
        Long version = this.memberToGCVersion.get(entry.getKey());
        if (version == null || version.longValue() < entry.getValue().longValue()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * locks against new version generation and returns the current region version number
   *
   */
  public long lockForClear(String regionPath, DistributionManager dm,
      InternalDistributedMember locker) {
    lockVersionGeneration(regionPath, dm, locker);
    return this.localVersion.get();
  }

  /** unlocks version generation for clear() operations */
  public void unlockForClear(InternalDistributedMember locker) {
    synchronized (this.clearLockSync) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unlocking for clear, from member {} RVV {}", locker,
            System.identityHashCode(this));
      }
      if (this.lockOwner != null && !locker.equals(this.lockOwner)) {
        if (logger.isDebugEnabled()) {
          logger.debug("current clear lock owner was {} not unlocking", lockOwner);
        }
        // this method is invoked by memberDeparted events and may not be for the current lock owner
        return;
      }
      unlockVersionGeneration(locker);
    }
  }

  /**
   * This schedules a thread that owns the version-generation write-lock for this vector. The method
   * unlockVersionGeneration notifies the thread to release the lock and terminate its run.
   *
   * @param dm the distribution manager - used to obtain an executor to hold the thread
   * @param locker the member requesting the lock (currently not used)
   */
  private void lockVersionGeneration(final String regionPath, final DistributionManager dm,
      final InternalDistributedMember locker) {
    final CountDownLatch acquiredLock = new CountDownLatch(1);
    if (logger.isDebugEnabled()) {
      logger.debug("Locking version generation for {} region {} RVV {}", locker, regionPath,
          System.identityHashCode(this));
    }
    // this could block for a while if a limit has been set on the waiting-thread-pool
    dm.getWaitingThreadPool().execute(new Runnable() {
      @edu.umd.cs.findbugs.annotations.SuppressWarnings(
          value = {"UL_UNRELEASED_LOCK", "IMSE_DONT_CATCH_IMSE"})
      public void run() {
        boolean haveLock = false;
        synchronized (clearLockSync) {
          try {
            // TODO Following code does not seem necessary as dlock has been taken
            // so no two threads will try to enter in this code section.
            while (locked && dm.isCurrentMember(locker)) {
              try {
                clearLockSync.wait();
              } catch (InterruptedException e) {
                // okay to ignore - release the lock and exit
              }
            }
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Waiting thread is now locking version generation for {} region {} RVV {}",
                  locker, regionPath, System.identityHashCode(this));
            }
            try {
              versionLock.writeLock().lock();
              lockOwner = locker;
              doUnlock = false;
              locked = true;
              haveLock = true;
              acquiredLock.countDown();
            } catch (IllegalMonitorStateException e) {
              // dlock on the clear() operation should prevent this from happening
              logger.fatal(LocalizedMessage.create(LocalizedStrings.RVV_LOCKING_CONFUSED,
                  new Object[] {locker, lockOwner}));
              return;
            }

            while (!doUnlock && dm.isCurrentMember(locker)) {
              try {
                clearLockSync.wait(250);
              } catch (InterruptedException e) {
                // okay to ignore - release the lock and exit
              }
            }
          } finally {
            if (haveLock) {
              locked = false; // this must be clear when the writeLock is released
                              // so we don't get warnings about it still being locked
              versionLock.writeLock().unlock();
              doUnlock = false;
              clearLockSync.notifyAll();
              // leave lockOwner set so we can see who the last lock request came from
            }
            acquiredLock.countDown();
          }
        }
      }
    });
    boolean interrupted = false;
    while (dm.isCurrentMember(locker)) {
      try {
        if (acquiredLock.await(250, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Done locking");
    }

  }

  private void unlockVersionGeneration(final InternalDistributedMember locker) {
    synchronized (clearLockSync) {
      this.doUnlock = true;
      this.clearLockSync.notifyAll();
    }
  }

  /**
   * return the next local version number
   */
  public long getNextVersion() {
    return getNextVersion(true);
  }

  /**
   * return the next local version number for a clear() operation, bypassing lock checks
   */
  public long getNextVersionWhileLocked() {
    return getNextVersion(false);
  }

  /**
   * return the next local version number
   */
  private long getNextVersion(boolean checkLocked) {
    if (checkLocked && this.locked) {
      // this should never be the case. If version generation is locked and we get here
      // then the path to this point is not protected by getting the version generation
      // lock from the RVV but it should be
      if (logger.isDebugEnabled()) {
        logger.debug("Generating a version tag when version generation is locked by {}",
            this.lockOwner);
      }
    }
    long new_version = localVersion.incrementAndGet();
    // since there could be special exception, we have to use recordVersion()
    recordVersion(getOwnerId(), new_version);
    return new_version;
  }

  /** obtain a lock to prevent concurrent clear() from happening */
  public void lockForCacheModification(LocalRegion owner) {
    if (owner.getServerProxy() == null) {
      this.versionLock.readLock().lock();
    }
  }

  /** release the lock preventing concurrent clear() from happening */
  public void releaseCacheModificationLock(LocalRegion owner) {
    if (owner.getServerProxy() == null) {
      this.versionLock.readLock().unlock();
    }
  }

  /** obtain a lock to prevent concurrent clear() from happening */
  public void lockForCacheModification() {
    this.versionLock.readLock().lock();
  }

  /** release the lock preventing concurrent clear() from happening */
  public void releaseCacheModificationLock() {
    this.versionLock.readLock().unlock();
  }

  private void syncLocalVersion() {
    long v = localVersion.get();
    synchronized (localExceptions) {
      if (v != localExceptions.version) {
        if (logger.isDebugEnabled()) {
          logger.debug("Adjust localExceptions.version {} to equal localVersion {}",
              localExceptions.version, localVersion.get());
        }
        localExceptions.version = v;
      }
    }
  }

  /**
   * return the current version for this member
   */
  public long getCurrentVersion() {
    synchronized (localExceptions) {
      syncLocalVersion();
      return localExceptions.getVersion();
    }
  }

  /**
   * return the current version for this member
   */
  public RegionVersionHolder<T> getLocalExceptions() {
    return localExceptions;
  }

  /**
   * return version holder for this member
   */
  public RegionVersionHolder<T> getHolderForMember(T id) {
    if (id.equals(this.myId)) {
      return localExceptions;
    } else {
      return this.memberToVersion.get(id);
    }
  }

  /**
   * returns the ID of the member that owns this version vector
   */
  public T getOwnerId() {
    return this.myId;
  }


  /**
   * turns off recording of versions for this vector. This can be used when recording of versions is
   * not necessary, as in an empty region
   */
  public void turnOffRecordingForEmptyRegion() {
    this.recordingDisabled = true;
  }

  /**
   * client version vectors only record GC numbers and don't keep exceptions, etc, because there
   * could be MANY of them
   */
  public void setIsClientVector() {
    this.clientVector = true;
  }

  /**
   * record all of the version information from an initial image provider
   */
  public void recordVersions(RegionVersionVector<T> otherVector) {
    synchronized (this.memberToVersion) {
      for (Map.Entry<T, RegionVersionHolder<T>> entry : otherVector.getMemberToVersion()
          .entrySet()) {
        T mbr = entry.getKey();
        RegionVersionHolder<T> otherHolder = entry.getValue();

        initializeVersionHolder(mbr, otherHolder);
      }
      // Get the set of local exceptions from the other vector
      // before directly accessing localExceptions, should sync its this.version with localVersion
      otherVector.syncLocalVersion();
      initializeVersionHolder(otherVector.getOwnerId(), otherVector.localExceptions);

      if (otherVector.getCurrentVersion() > 0
          && !this.memberToVersion.containsKey(otherVector.getOwnerId())) {
        recordVersion(otherVector.getOwnerId(), otherVector.getCurrentVersion());
      }

      // check if I have updates from members that the otherVector does not have
      // If yes, these are unfinished ops and should be cleaned
      for (T mbr : this.memberToVersion.keySet()) {
        if (!otherVector.memberToVersion.containsKey(mbr)
            && !mbr.equals(otherVector.getOwnerId())) {
          RegionVersionHolder holder = this.memberToVersion.get(mbr);
          initializeVersionHolder(mbr, new RegionVersionHolder(0));
        }
      }
      if (!otherVector.memberToVersion.containsKey(myId)
          && !myId.equals(otherVector.getOwnerId())) {
        initializeVersionHolder(myId, new RegionVersionHolder(0));
      }

      synchronized (this.memberToGCVersion) {
        for (Map.Entry<T, Long> entry : otherVector.getMemberToGCVersion().entrySet()) {
          T member = entry.getKey();
          Long value = entry.getValue();
          if (member.equals(myId)) {
            // If this entry is for our id, update our local GC version
            long currentValue;
            while ((currentValue = localGCVersion.get()) < value) {
              localGCVersion.compareAndSet(currentValue, value);
            }
          } else {
            // Update the memberToGCVersionMap.
            Long myVersion = this.memberToGCVersion.get(entry.getKey());
            if (myVersion == null || myVersion < entry.getValue()) {
              this.memberToGCVersion.put(entry.getKey(), entry.getValue());
            }
          }
        }
      }
    }
  }

  public void initializeVersionHolder(T mbr, RegionVersionHolder<T> otherHolder) {
    RegionVersionHolder<T> h = this.memberToVersion.get(mbr);
    if (h == null) {
      if (!mbr.equals(this.myId)) {
        h = otherHolder.clone();
        h.makeReadyForRecording();
        this.memberToVersion.put(mbr, h);
      } else {
        RegionVersionHolder<T> vh = otherHolder;
        long version = vh.version;
        updateLocalVersion(version);
        this.localExceptions.initializeFrom(vh);
      }
    } else {
      // holders must be modified under synchronization
      h.initializeFrom(otherHolder);
    }
  }

  void updateLocalVersion(long newVersion) {
    boolean needToTrySetAgain;
    do {
      needToTrySetAgain = false;
      long currentVersion = this.localVersion.get();
      if (currentVersion < newVersion) {
        needToTrySetAgain = !compareAndSetVersion(currentVersion, newVersion);
      }
    } while (needToTrySetAgain);
  }

  boolean compareAndSetVersion(long currentVersion, long newVersion) {
    return this.localVersion.compareAndSet(currentVersion, newVersion);
  }

  /**
   * Records a received region-version. These are transmitted in VersionTags in messages between
   * peers and from servers to clients.
   *
   * @param tag the version information
   */
  public void recordVersion(T mbr, VersionTag<T> tag) {
    tag.setRecorded();
    assert tag.isRecorded();
    T member = tag.getMemberID();
    if (member == null) {
      member = mbr;
    }

    if (this.myId.equals(member)) {
      // We can be asked to record a version for the local member if a persistent
      // member is restarted and an event is replayed after the persistent member
      // recovers. So we can only assert that the local member has already seen
      // the replayed event.
      synchronized (localExceptions) {
        if (this.localVersion.get() < tag.getRegionVersion() && region != null
            && region.isInitialized()) {
          Assert.fail(
              "recordVersion invoked for a local version tag that is higher than our local version. rvv="
                  + this + ", tag=" + tag + " " + region.getName());
        }
      }
    }

    recordVersion(member, tag.getRegionVersion());
  }

  /**
   * Records a received region-version. These are transmitted in VersionTags in messages between
   * peers and from servers to clients. In general you should use recordVersion(mbr, versionTag) so
   * that the tag is marked as having been recorded. This will keep
   * DistributedCacheOperation.basicProcess() from trying to record it again.
   *
   * This method is also called for versions which have been recovered from disk.
   *
   * @param member the peer that performed the operation
   * @param version the version of the peers region that reflects the operation
   */
  public void recordVersion(T member, long version) {
    T mbr = member;

    if (this.recordingDisabled || clientVector) {
      return;
    }

    RegionVersionHolder<T> holder;

    if (mbr.equals(this.myId)) {
      // If we are recording a version for the local member,
      // use the local exception list.
      holder = this.localExceptions;

      synchronized (holder) {
        // Advance the version held in the local
        // exception list to match the atomic long
        // we using for the local version.
        holder.version = this.localVersion.get();
      }
      updateLocalVersion(version);
    } else {
      // Find the version holder object
      holder = memberToVersion.get(mbr);
      if (holder == null) {
        synchronized (memberToVersion) {
          // Look for the holder under lock
          holder = memberToVersion.get(mbr);
          if (holder == null) {
            mbr = getCanonicalId(mbr);
            holder = new RegionVersionHolder<T>(mbr);
            memberToVersion.put(holder.id, holder);
          }
        }
      }
    }

    // Update the version holder
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "Recording rv{} for {}", version, mbr);
    }
    holder.recordVersion(version);
  }



  /**
   * Records a version holder that we have recovered from disk. This version holder replaces the
   * current version holder if it dominates the version holder we already have. This method will
   * called once for each oplog we recover.
   *
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ML_SYNC_ON_FIELD_TO_GUARD_CHANGING_THAT_FIELD",
      justification = "sync on localExceptions guards concurrent modification but this is a replacement")
  public void initRecoveredVersion(T member, RegionVersionHolder<T> v, boolean latestOplog) {
    RegionVersionHolder<T> recovered = v.clone();

    if (member == null || member.equals(myId)) {
      // if this is the version for the local member, update our local info

      // update the local exceptions
      synchronized (localExceptions) {
        // Fix for 45622 - We only take the version holder from the latest
        // oplog. There may be more than one RVV in the latest oplog, in which
        // case we want to end up with the last RVV from the latest oplog
        if (latestOplog || localVersion.get() == 0) {
          localExceptions = recovered;
          if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
            logger.trace(LogMarker.RVV_VERBOSE, "initRecoveredVersion setting local version to {}",
                recovered.version);
          }
          localVersion.set(recovered.version);
        }
      }
    } else {
      // If this is not a local member, update the member to version map
      Long gcVersion = memberToGCVersion.get(member);

      synchronized (memberToVersion) {
        RegionVersionHolder<T> oldVersion = memberToVersion.get(member);

        // Fix for 45622 - We only take the version holder from the latest
        // oplog. There may be more than one RVV in the latest oplog, in which
        // case we want to end up with the last RVV from the latest oplog
        if (latestOplog || oldVersion == null || oldVersion.version == 0) {
          if (gcVersion != null) {
            recovered.removeExceptionsOlderThan(gcVersion);
          }
          memberToVersion.put(member, recovered);
        }
      }
    }
  }

  /**
   * get the last recorded region version number for the given member
   */
  public long getVersionForMember(T mbr) {
    RegionVersionHolder<T> holder = this.memberToVersion.get(mbr);
    if (holder == null) {
      if (mbr.equals(myId)) {
        return getCurrentVersion();
      } else {
        return 0;
      }
    } else {
      return holder.getVersion();
    }
  }

  /**
   * Returns a list of the members that have been marked as having left the system.
   */
  public Set<T> getDepartedMembersSet() {
    synchronized (this.memberToVersion) {
      Set<T> result = new HashSet<T>();
      for (RegionVersionHolder<T> h : this.memberToVersion.values()) {
        if (h.isDepartedMember) {
          result.add((T) h.id);
        }
      }
      return result;
    }
  }


  /**
   * Test to see if this vector has seen the given version.
   *
   * @return true if this vector has seen the given version
   */
  public boolean contains(T id, long version) {
    if (id.equals(this.myId)) {
      if (isForSynchronization()) {
        // a sync vector only has one holder & no valid version for the vector's owner
        return true;
      }
      if (getCurrentVersion() < version) {
        return false;
      } else {
        return !localExceptions.hasExceptionFor(version);
      }
    }
    RegionVersionHolder<T> holder = this.memberToVersion.get(id);
    if (holder == null) {
      if (this.singleMember) {
        // we only care about missing changes from a particular member, and this
        // vector is known to contain that member's version holder
        return true;
      } else {
        return false;
      }
    } else {
      return holder.contains(version);
    }
  }

  /**
   * Removes departed members not in the given collection of IDs from the version vector
   *
   * @param idsToKeep collection of the kind of IDs appropriate for this vector
   */
  public void removeOldMembers(Set<VersionSource<T>> idsToKeep) {
    synchronized (this.memberToVersion) {
      for (Iterator<Map.Entry<T, RegionVersionHolder<T>>> it =
          this.memberToVersion.entrySet().iterator(); it.hasNext();) {
        Map.Entry<T, RegionVersionHolder<T>> entry = it.next();
        if (entry.getValue().isDepartedMember) {
          if (!idsToKeep.contains(entry.getKey())) {
            it.remove();
            this.memberToGCVersion.remove(entry.getKey());
            synchronized (this.canonicalIdLock) {
              this.canonicalIds.remove(entry.getKey());
            }
          }
        }
      }
    }
  }



  /**
   * Test hook - does this vector hold an entry for the given ID?
   */
  public boolean containsMember(T id) {
    if (this.memberToVersion.containsKey(id))
      return true;
    if (this.memberToGCVersion.containsKey(id))
      return true;
    if (this.canonicalIds.containsKey(id))
      return true;
    return false;
  }

  /**
   * This marks the given entry as departed, making it eligible to be removed during an operation
   * like DistributedRegion.synchronizeWith()
   *
   */
  protected void markDepartedMember(T id) {
    synchronized (this.memberToVersion) {
      RegionVersionHolder<T> holder = this.memberToVersion.get(id);
      if (holder != null) {
        holder.isDepartedMember = true;
      }
    }
  }

  /**
   * check to see if tombstone removal in this RVV indicates that tombstones have been removed from
   * its Region that have not been removed from the argument's Region. If this is the case, then a
   * delta GII may leave entries in the other RVV's Region that should be deleted.
   *
   * @return true if there have been tombstone removals in this vector's Region that were not done
   *         in the argument's region
   */
  public boolean hasHigherTombstoneGCVersions(RegionVersionVector<T> other) {
    if (this.localGCVersion.get() > 0) {
      Long version = other.memberToGCVersion.get(this.myId);
      if (version == null) {
        return true; // this vector has removed locally created tombstones that the other hasn't
                     // reaped
      } else if (this.localGCVersion.get() > version.longValue()) {
        return true;
      }
    }
    // see if I have members with GC versions that the other vector doesn't have
    for (T mbr : this.memberToGCVersion.keySet()) {
      if (!other.memberToGCVersion.containsKey(mbr)) {
        if (!mbr.equals(other.getOwnerId())) {
          return true;
        }
      }
    }
    // see if the other vector has members that have been removed from this
    // vector. If this happens we don't know if tombstones were removed
    for (T id : other.memberToGCVersion.keySet()) {
      if (!id.equals(this.myId) && !this.memberToGCVersion.containsKey(id)) {
        return true;
      }
    }
    // now see if I have anything newer for things we have in common
    for (Map.Entry<T, Long> entry : other.memberToGCVersion.entrySet()) {
      Long version = this.memberToGCVersion.get(entry.getKey());
      if (version != null) {
        Long otherVersion = entry.getValue();
        if (version.longValue() > otherVersion.longValue()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Test to see if this vector's region may be able to provide updates that the given vector has
   * not seen. This method assumes that the argument is not a live vector and requires no
   * synchronization.
   */
  public boolean isNewerThanOrCanFillExceptionsFor(RegionVersionVector<T> other) {
    if (other.singleMember) {
      // do the diff for only a single member. This is typically a member that
      // recently crashed.
      Map.Entry<T, RegionVersionHolder<T>> entry =
          other.memberToVersion.entrySet().iterator().next();
      RegionVersionHolder<T> holder = this.memberToVersion.get(entry.getKey());
      if (holder == null) {
        return false;
      } else {
        RegionVersionHolder<T> otherHolder = entry.getValue();
        return holder.isNewerThanOrCanFillExceptionsFor(otherHolder);
      }
    }
    // check my own updates
    if (getCurrentVersion() > 0) {
      RegionVersionHolder<T> otherHolder = other.memberToVersion.get(this.myId);
      if (otherHolder == null) {
        return true;
      }
      if (localExceptions.isNewerThanOrCanFillExceptionsFor(otherHolder)) {
        return true;
      }
    }
    // now see if I have anything newer for things we have in common
    for (Map.Entry<T, RegionVersionHolder<T>> entry : this.memberToVersion.entrySet()) {
      T mbr = entry.getKey();
      RegionVersionHolder<T> holder = entry.getValue();
      RegionVersionHolder<T> otherHolder = other.memberToVersion.get(mbr);
      if (otherHolder != null) {
        if (holder.isNewerThanOrCanFillExceptionsFor(otherHolder)) {
          return true;
        }
      } else if (mbr.equals(other.getOwnerId())) {
        if (holder.isNewerThanOrCanFillExceptionsFor(other.localExceptions)) {
          return true;
        }
      } else {
        // mbr = entry.getKey() does not exist in other
        return true;
      }
    }
    return false;
  }

  private boolean isGCVersionDominatedByHolder(Long gcVersion, RegionVersionHolder<T> otherHolder) {
    if (gcVersion == null || gcVersion.longValue() == 0) {
      return true;
    } else {
      RegionVersionHolder<T> holder = new RegionVersionHolder<T>(gcVersion.longValue());
      return !holder.isNewerThanOrCanFillExceptionsFor(otherHolder);
    }
  }

  /**
   * Test to see if this vector's rvvgc has updates that has not seen.
   */
  public synchronized boolean isRVVGCDominatedBy(RegionVersionVector<T> other) {
    if (other.singleMember) {
      // do the diff for only a single member. This is typically a member that
      // recently crashed.
      Map.Entry<T, RegionVersionHolder<T>> entry =
          other.memberToVersion.entrySet().iterator().next();

      Long gcVersion = this.memberToGCVersion.get(entry.getKey());
      return isGCVersionDominatedByHolder(gcVersion, entry.getValue());
    }

    boolean isDominatedByRemote = true;
    long localgcversion = this.localGCVersion.get();
    if (localgcversion > 0) {
      RegionVersionHolder<T> otherHolder = other.memberToVersion.get(this.myId);
      isDominatedByRemote = isGCVersionDominatedByHolder(localgcversion, otherHolder);
      if (isDominatedByRemote == false) {
        return false;
      }
    }

    for (Map.Entry<T, Long> entry : this.memberToGCVersion.entrySet()) {
      T mbr = entry.getKey();
      Long gcVersion = entry.getValue();
      RegionVersionHolder<T> otherHolder = null;
      if (mbr.equals(other.getOwnerId())) {
        otherHolder = localExceptions;
      } else {
        otherHolder = other.memberToVersion.get(mbr);
      }
      isDominatedByRemote = isGCVersionDominatedByHolder(gcVersion, otherHolder);
      if (isDominatedByRemote == false) {
        return false;
      }
    }
    return isDominatedByRemote;
  }

  /**
   * wait for this vector to dominate the given vector. This means that the receiver has seen all
   * version changes that the given vector has seen.
   *
   * @param otherVector the vector, usually from another member, that we want to dominate
   * @param region the region owning this vector
   * @return true if dominance was achieved
   */
  public boolean waitToDominate(RegionVersionVector<T> otherVector, LocalRegion region) {
    if (otherVector == this) {
      return true;
    }
    boolean result = false;
    long waitTimeRemaining = 0;
    long startTime = System.currentTimeMillis();
    boolean interrupted = false;
    CancelCriterion stopper = region.getCancelCriterion();
    try {
      do {
        stopper.checkCancelInProgress(null);
        result = dominates(otherVector);
        if (!result) {
          long now = System.currentTimeMillis();
          waitTimeRemaining = MAX_DOMINANCE_WAIT_TIME - (now - startTime);
          if (logger.isTraceEnabled()) {
            logger.trace("Waiting up to {} ms to achieve dominance", waitTimeRemaining);
          }
          if (waitTimeRemaining > 0) {
            long waitTime = Math.min(DOMINANCE_PAUSE_TIME, waitTimeRemaining);
            try {
              Thread.sleep(waitTime);
            } catch (InterruptedException e) {
              stopper.checkCancelInProgress(e);
              interrupted = true;
              break;
            }
          }
        }
      } while (waitTimeRemaining > 0 && !result);
    } finally {
      if (interrupted) {
        if (logger.isTraceEnabled()) {
          logger.trace("waitForDominance has been interrupted");
        }
        Thread.currentThread().interrupt();
      }
    }
    return result;
  }


  /** return true if this vector has seen all version changes that the other vector has seen */
  public boolean dominates(RegionVersionVector<T> other) {
    return !other.isNewerThanOrCanFillExceptionsFor(this);
  }

  /**
   * Remove any exceptions for the given member that are older than the given version. This is used
   * after a synchronization operation to get rid of unneeded history.
   *
   */
  public void removeExceptionsFor(DistributedMember mbr, long version) {
    RegionVersionHolder<T> holder = this.memberToVersion.get(mbr);
    if (holder != null) {
      synchronized (holder) {
        holder.removeExceptionsOlderThan(version);
      }
    }
  }

  /**
   * This is used by clear() while version generation is locked to remove old exceptions and update
   * the GC vector to be the same as the current version vector
   */
  public void removeOldVersions() {
    synchronized (this.memberToVersion) {
      long currentVersion = getCurrentVersion();
      for (Map.Entry<T, RegionVersionHolder<T>> entry : this.memberToVersion.entrySet()) {
        RegionVersionHolder<T> holder = entry.getValue();
        T id = entry.getKey();
        holder.removeExceptionsOlderThan(holder.version);
        this.memberToGCVersion.put(id, Long.valueOf(holder.version));
      }
      this.localGCVersion.set(getCurrentVersion());
      if (this.localExceptions != null) {
        synchronized (this.localExceptions) {
          this.localExceptions.removeExceptionsOlderThan(currentVersion);
        }
      }
    }
  }

  /**
   * Test hook
   */
  public int getExceptionCount(T mbr) {
    if (mbr.equals(this.myId)) {
      return localExceptions.getExceptionCount();
    }
    RegionVersionHolder<T> h = this.memberToVersion.get(mbr);
    if (h == null) {
      throw new IllegalStateException("there should be a holder for " + mbr);
    }
    return h.getExceptionCount();
  }

  /**
   * after deserializing a version tag or RVV the IDs in it should be replaced with references to
   * IDs returned by this method. This vastly reduces the memory footprint of tags/stamps/rvvs
   *
   * @return the canonical reference
   */
  public T getCanonicalId(T id) {
    if (id == null) {
      return null;
    } else if (id.equals(myId)) {
      return myId;
    } else {
      T can = id;
      T cId = this.canonicalIds.get(can);
      if (cId != null) {
        return cId;
      }
      if (id instanceof InternalDistributedMember) {
        InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
        if (system != null) {
          can = (T) system.getDistributionManager().getCanonicalId((InternalDistributedMember) id);
        }
      }
      synchronized (this.canonicalIdLock) {
        HashMap<T, T> tmp = new HashMap<T, T>(this.canonicalIds);
        tmp.put(can, can);
        this.canonicalIds = tmp;
      }
      return can;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.DataSerializableFixedID#toData(java.io.DataOutput)
   */
  public void toData(DataOutput out) throws IOException {
    if (this.isLiveVector) {
      throw new IllegalStateException("serialization of this object is not allowed");
    }
    writeMember(this.myId, out);
    int flags = 0;
    if (this.singleMember) {
      flags |= 0x01;
    }
    out.writeInt(flags);
    out.writeLong(this.localVersion.get());
    out.writeLong(this.localGCVersion.get());
    out.writeInt(this.memberToVersion.size());
    for (Map.Entry<T, RegionVersionHolder<T>> entry : this.memberToVersion.entrySet()) {
      writeMember(entry.getKey(), out);
      InternalDataSerializer.invokeToData(entry.getValue(), out);
    }
    out.writeInt(this.memberToGCVersion.size());
    for (Map.Entry<T, Long> entry : this.memberToGCVersion.entrySet()) {
      writeMember(entry.getKey(), out);
      out.writeLong(entry.getValue());
    }
    InternalDataSerializer.invokeToData(this.localExceptions, out);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.DataSerializableFixedID#fromData(java.io.DataInput)
   */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.myId = readMember(in);
    int flags = in.readInt();
    this.singleMember = ((flags & 0x01) == 0x01);
    this.localVersion.set(in.readLong());
    this.localGCVersion.set(in.readLong());
    int numHolders = in.readInt();
    for (int i = 0; i < numHolders; i++) {
      T key = readMember(in);
      RegionVersionHolder<T> holder = new RegionVersionHolder<T>(in);
      holder.id = key;
      this.memberToVersion.put(key, holder);
    }
    int numGCVersions = in.readInt();
    for (int i = 0; i < numGCVersions; i++) {
      T key = readMember(in);
      RegionVersionHolder<T> holder = this.memberToVersion.get(key);
      if (holder != null) {
        key = holder.id;
      } // else it could go in canonicalIds, but that's not used in copies of RVVs
      long value = in.readLong();
      this.memberToGCVersion.put(key, value);
    }
    this.localExceptions = new RegionVersionHolder<T>(in);
  }

  protected abstract void writeMember(T member, DataOutput out) throws IOException;

  protected abstract T readMember(DataInput in) throws IOException, ClassNotFoundException;

  /**
   * When a tombstone is removed from the entry map this method must be called to record the max
   * region-version of any tombstone reaped. Any older versions are then immediately eligible for
   * reaping.
   *
   */
  public void recordGCVersion(T mbr, long regionVersion) {
    if (mbr == null) {
      mbr = this.myId;
    }
    // record the GC version to make sure we know we have seen this version
    // during recovery, this will prevent us from recording exceptions
    // for entries less than the GC version.
    recordVersion(mbr, regionVersion);
    if (mbr == null || mbr.equals(this.myId)) {
      boolean succeeded;
      do {
        long v = localGCVersion.get();
        if (v > regionVersion) {
          break;
        }
        succeeded = localGCVersion.compareAndSet(v, regionVersion);
      } while (!succeeded);
    } else {
      synchronized (this.memberToGCVersion) {
        Long holder = this.memberToGCVersion.get(mbr);
        if (holder != null) {
          this.memberToGCVersion.put(mbr, Math.max(regionVersion, holder));
        } else {
          this.memberToGCVersion.put(mbr, regionVersion);
        }
      }
    }
  }

  /**
   * record all of the GC versions in the given vector
   *
   */
  public void recordGCVersions(RegionVersionVector<T> other) {
    assert other.memberToGCVersion != null : "incoming gc version set is null";
    recordGCVersion(other.myId, other.localGCVersion.get());
    for (Map.Entry<T, Long> entry : other.memberToGCVersion.entrySet()) {
      recordGCVersion(entry.getKey(), entry.getValue().longValue());
    }
  }

  /**
   * returns true if tombstones newer than the given version have already been reaped. This means
   * that a clear or GC has been received that should have wiped out the operation this version
   * stamp represents, but this operation had not yet been received
   *
   * @return true if the given version should be rejected
   */
  public boolean isTombstoneTooOld(T mbr, long gcVersion) {
    Long newestReapedVersion;
    if (mbr == null || mbr.equals(myId)) {
      newestReapedVersion = this.localGCVersion.get();
    } else {
      newestReapedVersion = this.memberToGCVersion.get(mbr);
    }
    if (newestReapedVersion != null) {
      return (newestReapedVersion.longValue() >= gcVersion);
    }
    return false;
  }

  /**
   * returns the highest region-version of any tombstone owned by the given member that was reaped
   * in this vector's region
   */
  public long getGCVersion(T mbr) {
    if (mbr == null || mbr.equals(this.myId)) {
      return localGCVersion.get();
    } else {
      synchronized (this.memberToGCVersion) {
        Long holder = this.memberToGCVersion.get(mbr);
        if (holder != null) {
          return holder.longValue();
        }
        return -1;
      }
    }
  }

  /**
   * Get a map of the member to the version and exception list for that member, including the local
   * member.
   */
  public Map<T, RegionVersionHolder<T>> getMemberToVersion() {
    RegionVersionHolder<T> myExceptions;
    myExceptions = this.localExceptions.clone();

    HashMap<T, RegionVersionHolder<T>> results =
        new HashMap<T, RegionVersionHolder<T>>(memberToVersion);

    results.put(getOwnerId(), myExceptions);
    return results;
  }

  /**
   * Get a map of member to the GC version of that member, including the local member.
   */
  public synchronized Map<T, Long> getMemberToGCVersion() {
    HashMap<T, Long> results = new HashMap<T, Long>(memberToGCVersion);
    if (localGCVersion.get() > 0) {
      results.put(getOwnerId(), localGCVersion.get());
    }
    return results;
  }

  /**
   * Remove an exceptions that are older than the current GC version for each member in the RVV.
   */
  public void pruneOldExceptions() {
    Set<T> members;
    members = new HashSet<T>(memberToGCVersion.keySet());

    for (T member : members) {
      Long gcVersion = memberToGCVersion.get(member);

      RegionVersionHolder<T> holder;
      holder = memberToVersion.get(member);

      if (holder != null && gcVersion != null) {
        synchronized (holder) {
          holder.removeExceptionsOlderThan(gcVersion);
        }
      }
    }

    localExceptions.removeExceptionsOlderThan(localGCVersion.get());
  }

  @Override
  public String toString() {
    if (this.isLiveVector) {
      return "RegionVersionVector{rv" + this.localVersion + " gc" + this.localGCVersion + "}@"
          + System.identityHashCode(this);
    } else {
      return fullToString();
    }
  }

  /** this toString method is not thread-safe */
  public String fullToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RegionVersionVector[").append(this.myId).append("={rv")
        .append(this.localExceptions.version).append(" gc" + this.localGCVersion)
        .append(" localVersion=" + this.localVersion);
    try {
      sb.append(" local exceptions=" + this.localExceptions.exceptionsToString());
    } catch (ConcurrentModificationException c) {
      sb.append(" (unable to access local exceptions)");
    }
    sb.append("} others=");
    String mbrVersions = "";
    try {
      mbrVersions = this.memberToVersion.toString();
    } catch (ConcurrentModificationException e) {
      mbrVersions = "(unable to access)";
    }
    sb.append(mbrVersions);
    if (this.memberToGCVersion != null) {
      try {
        mbrVersions = this.memberToGCVersion.toString();
      } catch (ConcurrentModificationException e) {
        mbrVersions = "(unable to access)";
      }
      sb.append(", gc=").append(mbrVersions);
    }
    sb.append("]");
    return sb.toString();
  }


  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {}

  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  /*
   * (non-Javadoc) this ensures that the version generation lock is released
   *
   * @see org.apache.geode.distributed.internal.MembershipListener#memberDeparted(org.apache.geode.
   * distributed.internal.membership.InternalDistributedMember, boolean)
   */
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, boolean crashed) {
    // since unlockForClear uses synchronization we need to try to execute it in another
    // thread so that membership events aren't blocked
    if (distributionManager != null) {
      distributionManager.getWaitingThreadPool().execute(new Runnable() {
        public void run() {
          unlockForClear(id);
        }
      });
    } else {
      unlockForClear(id);
    }
  }

  public static RegionVersionVector<?> create(boolean persistent, DataInput in)
      throws IOException, ClassNotFoundException {
    RegionVersionVector<?> rvv;
    if (persistent) {
      rvv = new DiskRegionVersionVector();
    } else {
      rvv = new VMRegionVersionVector();
    }
    InternalDataSerializer.invokeFromData(rvv, in);
    return rvv;
  }

  public static RegionVersionVector<?> create(VersionSource<?> versionMember, LocalRegion owner) {
    if (versionMember instanceof DiskStoreID) {
      return new DiskRegionVersionVector((DiskStoreID) versionMember, owner);
    } else {
      return new VMRegionVersionVector((InternalDistributedMember) versionMember, owner);
    }
  }

  /**
   * For test purposes, see if two RVVs have seen the same events and GC version vectors
   *
   * @return true if the RVVs are the same.
   */
  public boolean sameAs(RegionVersionVector<T> other) {
    // Compare the version version vectors
    Map<T, RegionVersionHolder<T>> myMemberToVersion = getMemberToVersion();
    Map<T, RegionVersionHolder<T>> otherMemberToVersion = other.getMemberToVersion();

    if (!myMemberToVersion.keySet().equals(otherMemberToVersion.keySet())) {
      return false;
    }
    for (Iterator<T> it = myMemberToVersion.keySet().iterator(); it.hasNext();) {
      T key = it.next();
      if (!myMemberToVersion.get(key).sameAs(otherMemberToVersion.get(key))) {
        return false;
      }
    }

    Map<T, Long> myGCVersion = getMemberToGCVersion();
    Map<T, Long> otherGCVersion = other.getMemberToGCVersion();

    if (!myGCVersion.equals(otherGCVersion)) {
      return false;
    }

    return true;
  }

  /**
   * Note: test only. If put in production will cause ConcurrentModificationException see if two
   * RVVs have seen the same events and GC version vectors This will treat member with null version
   * the same as member with version=0
   *
   * @return true if the RVVs are the same logically.
   */
  public boolean logicallySameAs(RegionVersionVector<T> other) {
    return (this.dominates(other) && other.dominates(this));
  }


  /**
   * @return true if this vector represents the entry for a single member to be used in
   *         synchronizing caches for that member in the case of a crash. Otherwise return false.
   */
  public boolean isForSynchronization() {
    return this.singleMember;
  }

  /**
   * Test hook - see if a member is marked as "departed"
   */
  public boolean isDepartedMember(VersionSource<T> mbr) {
    RegionVersionHolder<T> h = memberToVersion.get(mbr);
    return (h != null) && h.isDepartedMember;
  }

  public Version[] getSerializationVersions() {
    return null;
  }

  // /**
  // * This class will wrap DM member IDs to provide integers that can be stored
  // * on disk and be timed out in the vector.
  // *
  // *
  // */
  // static class RVVMember implements Comparable {
  // private static AtomicLong NextId = new AtomicLong();
  // T memberId;
  // long timeAdded;
  // long internalId;
  //
  // RVVMember(T m, long timeAdded, long internalId) {
  // this.memberId = m;
  // this.timeAdded = timeAdded;
  // this.internalId = internalId;
  // if (NextId.get() < internalId) {
  // NextId.set(internalId);
  // }
  // }
  //
  // RVVMember(T m) {
  // this.memberId = m;
  // this.timeAdded = System.currentTimeMillis();
  // this.internalId = NextId.incrementAndGet();
  // }
  //
  // public int compareTo(Object o) {
  // if (o instanceof T) {
  // return -((T)o).compareTo(this.memberId);
  // } else {
  // return this.memberId.compareTo(((RVVMember)o).memberId);
  // }
  // }
  //
  // @Override
  // public int hashCode() {
  // return this.memberId.hashCode();
  // }
  //
  // @Override
  // public boolean equals(Object o) {
  // if (o instanceof T) {
  // return ((T)o).equals(this.memberId);
  // } else {
  // return this.memberId.equals(((RVVMember)o).memberId);
  // }
  // }
  //
  // @Override
  // public String toString() {
  // return "vID(#"+this.internalId+"; time="+this.timeAdded+"; id="+this.memberId+")";
  // }
  // }



}
