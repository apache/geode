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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.PartitionedRegion.BucketLock;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.CreateBucketResult;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMembershipView;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

/**
 * Empty shell for {@link BucketRegion} which exists only to maintain metadata in the form of a
 * {@link BucketAdvisor}
 *
 * @since GemFire 5.1
 */
public class ProxyBucketRegion implements Bucket {
  private static final Logger logger = LogService.getLogger();

  private final int serialNumber;
  private final int bid;
  private final PartitionedRegion partitionedRegion;
  private final BucketAdvisor advisor;
  private final BucketPersistenceAdvisor persistenceAdvisor;
  private volatile BucketRegion realBucket = null;
  private final AtomicBoolean bucketSick = new AtomicBoolean(false);
  private final Set<DistributedMember> sickHosts = new HashSet<DistributedMember>();
  private final DiskRegion diskRegion;
  private final BucketLock bucketLock;

  /**
   * Note that LocalRegion has a version of this name spelled "NO_PARTITITON". So if code is written
   * that compares to this constant make sure to also compare to the other one from LocalRegion. The
   * one in LocalRegion is a typo but has already been persisted in older versions.
   */
  public static final String NO_FIXED_PARTITION_NAME = "NO_PARTITION";

  /**
   * Constructs a new ProxyBucketRegion which has a BucketAdvisor.
   *
   * @param bid the bucket id
   * @param partitionedRegion the PartitionedRegion that owns this bucket
   * @param internalRegionArgs the internal args which includes RegionAdvisor
   */
  public ProxyBucketRegion(int bid, PartitionedRegion partitionedRegion,
      InternalRegionArguments internalRegionArgs) {
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.bid = bid;
    this.partitionedRegion = partitionedRegion;
    this.advisor =
        BucketAdvisor.createBucketAdvisor(this, internalRegionArgs.getPartitionedRegionAdvisor());

    this.bucketLock = this.partitionedRegion.getBucketLock(this.bid);

    if (this.partitionedRegion.getDataPolicy().withPersistence()) {

      String regionPath = getFullPath();
      PersistentMemberManager memberManager =
          partitionedRegion.getGemFireCache().getPersistentMemberManager();
      DiskRegionStats diskStats = partitionedRegion.getDiskRegionStats();
      DistributedLockService dl =
          partitionedRegion.getGemFireCache().getPartitionedRegionLockService();
      DiskStoreImpl ds = partitionedRegion.getDiskStore();
      EvictionAttributes ea = partitionedRegion.getAttributes().getEvictionAttributes();
      EnumSet<DiskRegionFlag> diskFlags = EnumSet.noneOf(DiskRegionFlag.class);
      // Add flag if this region has versioning enabled
      if (partitionedRegion.getConcurrencyChecksEnabled()) {
        diskFlags.add(DiskRegionFlag.IS_WITH_VERSIONING);
      }
      boolean overflowEnabled = ea != null && ea.getAction().isOverflowToDisk();
      int startingBucketID = -1;
      String partitionName = NO_FIXED_PARTITION_NAME;
      List<FixedPartitionAttributesImpl> fpaList =
          partitionedRegion.getFixedPartitionAttributesImpl();
      if (fpaList != null) {
        for (FixedPartitionAttributesImpl fpa : fpaList) {
          if (fpa.hasBucket(bid)) {
            startingBucketID = fpa.getStartingBucketID();
            partitionName = fpa.getPartitionName();
            break;
          }
        }
      }
      this.diskRegion =
          DiskRegion.create(ds, regionPath, true, partitionedRegion.getPersistBackup(),
              overflowEnabled, partitionedRegion.isDiskSynchronous(),
              partitionedRegion.getDiskRegionStats(), partitionedRegion.getCancelCriterion(),
              partitionedRegion, partitionedRegion.getAttributes(), diskFlags, partitionName,
              startingBucketID, partitionedRegion.getCompressor(), partitionedRegion.getOffHeap());

      if (fpaList != null) {
        for (FixedPartitionAttributesImpl fpa : fpaList) {
          if (fpa.getPartitionName().equals(this.diskRegion.getPartitionName())
              && this.diskRegion.getStartingBucketId() != -1) {
            fpa.setStartingBucketID(this.diskRegion.getStartingBucketId());
            partitionedRegion.getPartitionsMap().put(fpa.getPartitionName(),
                new Integer[] {fpa.getStartingBucketID(), fpa.getNumBuckets()});
          }
        }
      }

      this.persistenceAdvisor = new BucketPersistenceAdvisor(advisor, dl, diskRegion, regionPath,
          diskStats, memberManager, bucketLock, this);
    } else {
      this.diskRegion = null;
      this.persistenceAdvisor = null;
    }
  }

  public CancelCriterion getCancelCriterion() {
    return this.partitionedRegion.getCache().getCancelCriterion();
  }

  public void close() {
    if (this.persistenceAdvisor != null) {
      this.persistenceAdvisor.close();
    }
    this.advisor.closeAdvisor();
    if (this.diskRegion != null) {
      this.diskRegion.close(null);
    }
  }

  public int getSerialNumber() {
    // always return the serial number for this proxy, NOT the bucket region
    return this.serialNumber;
  }

  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  public DistributionAdvisor getDistributionAdvisor() {
    return this.advisor;
  }

  public CacheDistributionAdvisor getCacheDistributionAdvisor() {
    return this.advisor;
  }

  public Profile getProfile() {
    return this.advisor.createProfile();
  }

  public DistributionAdvisee getParentAdvisee() {
    return this.partitionedRegion;
  }

  public PartitionedRegion getPartitionedRegion() {
    return this.partitionedRegion;
  }

  public InternalDistributedSystem getSystem() {
    return this.partitionedRegion.getCache().getInternalDistributedSystem();
  }

  public String getName() {
    return getPartitionedRegion().getBucketName(this.bid);
  }

  public String getFullPath() {
    return Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME + Region.SEPARATOR
        + getPartitionedRegion().getBucketName(this.bid);
  }

  public InternalCache getCache() {
    return this.partitionedRegion.getCache();
  }

  public RegionService getCacheView() {
    return this.partitionedRegion.getRegionService();
  }

  public RegionAttributes getAttributes() {
    return this.partitionedRegion.getAttributes();
  }

  public BucketAdvisor getBucketAdvisor() {
    return this.advisor;
  }

  /**
   * Notify this proxy of the real bucket as its target. Future calls to this instance will then
   * proxy them back to the real bucket.
   *
   * @param br the real bucket which will be the target for this proxy
   */
  public void setBucketRegion(BucketRegion br) {
    // fix several bugs including 36881... creation of BR may be occurring
    // at same time another thread is destroying the PR and now that this
    // BR is visible to the destroy thread we want to prevent sending bogus
    // CreateRegion or profile update messages
    this.partitionedRegion.checkReadiness();
    this.partitionedRegion.checkClosed();
    Assert.assertTrue(this.realBucket == null);
    Assert.assertTrue(!this.advisor.isHosting());
    this.realBucket = br;
  }

  public void clearBucketRegion(BucketRegion br) {
    Assert.assertTrue(this.realBucket == br);
    this.realBucket = null;
  }

  public void setHosting(boolean value) {
    if (value) {
      PartitionedRegion region = this.getPartitionedRegion();
      Assert.assertTrue(this.realBucket != null);
      Assert.assertTrue(!this.advisor.isHosting());
      if (region.isFixedPartitionedRegion()) {
        List<FixedPartitionAttributesImpl> list = region.getFixedPartitionAttributesImpl();
        if (list != null) {
          for (FixedPartitionAttributesImpl info : list) {
            if (info.hasBucket(bid)) {
              this.advisor.setHosting(true);
              break;
            }
          }
        }
      } else { // normal PR
        this.advisor.setHosting(true);
      }
    } else {
      // Assert.assertTrue(!getPartitionedRegion().getDataStore().isManagingBucket(this.bid));
      this.advisor.setHosting(false);
      this.realBucket = null;
    }
  }

  public void removeBucket() {
    this.realBucket.removeFromPeersAdvisors(true);
    this.advisor.removeBucket();
    this.realBucket = null;
  }

  /**
   * Get the redundancy of the this bucket, taking into account the local bucket, if any.
   *
   * @return number of redundant copies for a given bucket, or -1 if there are no instances of the
   *         bucket.
   */
  public int getBucketRedundancy() {
    return getBucketAdvisor().getBucketRedundancy();
  }

  public boolean isPrimary() {
    return this.advisor.isPrimary();
  }

  /**
   * Returns the real BucketRegion if one has been created. This call will return the bucket even if
   * it is still being initialized. Returns null if the bucket has not been created locally.
   *
   * @return the real bucket if currently created or null
   */
  public BucketRegion getCreatedBucketRegion() {
    return this.realBucket;
  }

  /**
   * Returns the real BucketRegion that is currently being locally hosted. Returns null if the real
   * bucket is null or if it is still being initialized. After the bucket is intialized isHosting
   * will be flagged true and future calls to this method will return the bucket.
   *
   * @return the real bucket if currently hosted or null
   */
  public BucketRegion getHostedBucketRegion() {
    if (this.advisor.isHosting()) {
      return this.realBucket;
    } else {
      return null;
    }
  }

  public boolean isHosting() {
    return this.advisor.isHosting();
  }

  public void fillInProfile(Profile profile) {
    if (logger.isDebugEnabled()) {
      logger.debug("ProxyBucketRegion filling in profile: {}", profile);
    }
    BucketRegion bucket = this.realBucket;
    if (bucket != null) {
      bucket.fillInProfile(profile);
    }
  }

  public ProxyBucketRegion initialize() {
    // dead coded initializationGate to prevent profile exchange
    // this.advisor.initializationGate();
    this.advisor.setInitialized();
    return this;
  }

  public Set<InternalDistributedMember> getBucketOwners() {
    Set<InternalDistributedMember> s = this.advisor.adviseInitialized();
    if (s == Collections.<InternalDistributedMember>emptySet()) {
      s = new HashSet<InternalDistributedMember>();
    }
    if (isHosting()) {
      s.add(this.partitionedRegion.getDistributionManager().getId());
    }
    return s;
  }

  /**
   * Returns the total number of datastores hosting an instance of this bucket.
   *
   * @return the total number of datastores hosting an instance of this bucket
   */
  public int getBucketOwnersCount() {
    return this.advisor.getBucketRedundancy() + 1;
  }

  public int getBucketId() {
    return this.bid;
  }

  public int getId() {
    return getBucketId();
  }

  public void setBucketSick(DistributedMember member, boolean sick) {
    synchronized (this.sickHosts) {
      if (sick) {
        this.sickHosts.add(member);
      } else {
        this.sickHosts.remove(member);
      }
      this.bucketSick.set(this.sickHosts.size() > 0);
    }
  }

  public boolean isBucketSick() {
    return this.bucketSick.get();
  }

  public Set<DistributedMember> getSickMembers() {
    synchronized (this.sickHosts) {
      return Collections.unmodifiableSet(new HashSet<DistributedMember>(this.sickHosts));
    }
  }

  public void recoverFromDiskRecursively() {
    recoverFromDisk();

    List<PartitionedRegion> colocatedWithList =
        ColocationHelper.getColocatedChildRegions(partitionedRegion);
    for (PartitionedRegion childPR : colocatedWithList) {
      if (childPR.getDataPolicy().withPersistence()) {
        ProxyBucketRegion[] childBucketArray = childPR.getRegionAdvisor().getProxyBucketArray();
        if (childBucketArray != null) {
          ProxyBucketRegion childBucket = childBucketArray[getBucketId()];
          childBucket.recoverFromDisk();
        }
      }
    }
  }

  public void recoverFromDisk() {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    RuntimeException exception = null;
    if (isDebugEnabled) {
      logger.debug("{} coming to recover from disk. wasHosting {}", getFullPath(),
          persistenceAdvisor.wasHosting());
    }
    try {
      if (persistenceAdvisor.wasHosting()) {
        if (isDebugEnabled) {
          logger.debug("{} used to host data. Attempting to recover.", getFullPath());
        }
        CreateBucketResult result;
        if (hasPersistentChildRegion()) {
          // If this is a parent PR, create the bucket, possibly going over
          // redundancy. We need to do this so that we can create the child
          // region in this member. This member may have the latest data for the
          // child region.
          result = partitionedRegion.getDataStore().grabBucket(bid,
              getDistributionManager().getDistributionManagerId(), true, true, false, null, true);
        } else {
          if (this.partitionedRegion.isShadowPR()
              && this.partitionedRegion.getColocatedWith() != null) {
            PartitionedRegion colocatedRegion =
                ColocationHelper.getColocatedRegion(this.partitionedRegion);

            if (this.partitionedRegion.getDataPolicy().withPersistence()
                && !colocatedRegion.getDataPolicy().withPersistence()) {
              result = colocatedRegion.getDataStore().grabBucket(bid,
                  getDistributionManager().getDistributionManagerId(), true, true, false, null,
                  true);

              if (result.nowExists()) {
                result = partitionedRegion.getDataStore().grabBucket(bid, null, true, false, false,
                    null, true);
              }
            } else {
              result = partitionedRegion.getDataStore().grabBucket(bid, null, true, false, false,
                  null, true);
            }
          } else {
            result = partitionedRegion.getDataStore().grabBucket(bid, null, true, false, false,
                null, true);
          }

        }
        if (result.nowExists()) {
          return;
        } else if (result != CreateBucketResult.REDUNDANCY_ALREADY_SATISFIED) {
          // TODO prpersist - check cache closure, create new error message
          this.partitionedRegion.checkReadiness();
          throw new InternalGemFireError(
              "Unable to restore the persistent bucket " + this.getName());
        }

        if (isDebugEnabled) {
          logger.debug(
              "{} redundancy is already satisfied, so discarding persisted data. Current hosts {}",
              getFullPath(), advisor.adviseReplicates());
        }

        // Destroy the data if we can't create the bucket, or if the redundancy is already satisfied
        destroyOfflineData();
      }

      if (isDebugEnabled) {
        logger.debug("{} initializing membership view from peers", getFullPath());
      }

      persistenceAdvisor.initializeMembershipView();
    } catch (DiskAccessException dae) {
      this.partitionedRegion.handleDiskAccessException(dae);
      throw dae;
    } catch (RuntimeException e) {
      exception = e;
      throw e;
    } finally {
      persistenceAdvisor.recoveryDone(exception);
    }
  }

  boolean hasPersistentChildRegion() {
    boolean hasPersistentChildRegion = ColocationHelper.hasPersistentChildRegion(partitionedRegion);
    return hasPersistentChildRegion;
  }

  /**
   * Destroy the offline data just for this bucket.
   */
  public void destroyOfflineData() {
    Map<InternalDistributedMember, PersistentMemberID> onlineMembers =
        advisor.adviseInitializedPersistentMembers();
    persistenceAdvisor.checkMyStateOnMembers(onlineMembers.keySet());
    diskRegion.beginDestroyDataStorage();
    persistenceAdvisor.finishPendingDestroy();
    if (logger.isDebugEnabled()) {
      logger.debug("destroyed persistent data for {}" + getFullPath());
    }
  }

  public BucketPersistenceAdvisor getPersistenceAdvisor() {
    return this.persistenceAdvisor;
  }

  public DiskRegion getDiskRegion() {
    return this.diskRegion;
  }

  public void finishRemoveBucket() {
    if (this.persistenceAdvisor != null) {
      this.persistenceAdvisor.bucketRemoved();
    }
  }

  public BucketLock getBucketLock() {
    return bucketLock;
  }

  public void initializePersistenceAdvisor() {
    persistenceAdvisor.initialize();

    List<PartitionedRegion> colocatedWithList =
        ColocationHelper.getColocatedChildRegions(partitionedRegion);
    for (PartitionedRegion childPR : colocatedWithList) {
      ProxyBucketRegion[] childBucketArray = childPR.getRegionAdvisor().getProxyBucketArray();
      if (childBucketArray != null) {
        ProxyBucketRegion childBucket = childBucketArray[getBucketId()];
        if (childBucket.persistenceAdvisor != null) {
          childBucket.persistenceAdvisor.initialize();
        }
      }
    }
  }

  public boolean checkBucketRedundancyBeforeGrab(InternalDistributedMember moveSource,
      boolean replaceOfflineData) {
    int redundancy = getBucketAdvisor().getBucketRedundancy();
    // Skip any checks if this is a colocated bucket. We need to create
    // the colocated bucket if we managed to create the parent bucket. There are
    // race conditions where the parent region may know that a member is no longer
    // hosting the bucket, but the child region doesn't know that yet.
    PartitionedRegion colocatedRegion = ColocationHelper.getColocatedRegion(this.partitionedRegion);
    if (colocatedRegion != null) {
      return true;
    }

    // Check for offline members, if the region has persistence
    // Even if we intend to replace offline data, we still need to make
    // sure the bucket isn't completely offline
    if (!replaceOfflineData || redundancy == -1) {
      BucketPersistenceAdvisor persistAdvisor = getPersistenceAdvisor();
      if (persistAdvisor != null) {
        // If we haven't finished recovering from disk, don't allow the bucket creation.
        // if(persistAdvisor.isRecovering()) {
        // return false;
        // }
        // If we previously hosted this bucket, go ahead and initialize
        // If this bucket never had a primary, go ahead and initialize,
        // any offline buckets should be empty
        if (!persistAdvisor.wasHosting() && advisor.getHadPrimary()) {
          final PersistentMembershipView membershipView = persistAdvisor.getMembershipView();
          if (membershipView == null) {
            // Fix for 42327 - There must be a race where we are being told to create a bucket
            // before we recover from disk. In that case, the membership view can be null.
            // Refuse to create the bucket if that is the case.
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "grabFreeBucket: Can't create bucket because persistence is not yet initialized {}{}{}",
                  this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bid);
            }
            return false;
          }
          Set<PersistentMemberID> offlineMembers = membershipView.getOfflineMembers();
          if (logger.isDebugEnabled()) {
            logger.debug(
                "We didn't host the bucket. Checking redundancy level before creating the bucket. Redundancy={} offline members={}",
                redundancy, offlineMembers);
          }

          if (offlineMembers != null && !offlineMembers.isEmpty() && redundancy == -1) {
            // If there are offline members, and no online members, throw
            // an exception indicating that we can't create the bucket.
            String message = LocalizedStrings.PartitionedRegionDataStore_DATA_OFFLINE_MESSAGE
                .toLocalizedString(partitionedRegion.getFullPath(), bid, offlineMembers);
            throw new PartitionOfflineException((Set) offlineMembers, message);
          } else {
            // If there are online and offline members, add the offline
            // members to the redundancy level. This way we won't create
            // an extra copy of the bucket.
            if (offlineMembers != null) {
              redundancy += offlineMembers.size();
            }
          }
        }
      }
    }

    if (moveSource == null) {
      if (redundancy >= this.partitionedRegion.getRedundantCopies()) {
        if (logger.isDebugEnabled()) {
          logger.debug("grabFreeBucket: Bucket already meets redundancy level bucketId={}{}{}",
              this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bid);
        }

        return false;
      }
    }

    // Check to see if this bucket is allowed on this source. If this
    // is a bucket move, we allow the source to be on the same host.
    if (!PartitionedRegionBucketMgmtHelper.bucketIsAllowedOnThisHost(this, moveSource)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "grabFreeBucket: Bucket can't be recovered because we're enforcing that the bucket host must be unique {}{}{}",
            this.partitionedRegion.getPRId(), PartitionedRegion.BUCKET_ID_SEPARATOR, bid);
      }
      return false;
    }
    return true;
  }

  public void waitForPrimaryPersistentRecovery() {
    persistenceAdvisor.waitForPrimaryPersistentRecovery();

  }

  public void initializePrimaryElector(InternalDistributedMember creationRequestor) {
    advisor.initializePrimaryElector(creationRequestor);

    if (persistenceAdvisor != null) {
      persistenceAdvisor.setAtomicCreation(creationRequestor != null);
    }
  }

  public void clearPrimaryElector() {
    if (persistenceAdvisor != null) {
      persistenceAdvisor.setAtomicCreation(false);
    }

  }

  @Override
  public void remoteRegionInitialized(CacheProfile profile) {
    // no-op for proxy bucket regions, which have no region membership listeners to notify
  }
}
