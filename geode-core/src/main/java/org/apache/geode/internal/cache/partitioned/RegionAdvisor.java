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

package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ProfileListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.BucketPersistenceAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.Node;
import org.apache.geode.internal.cache.PRHARedundancyProvider.DataStoreBuckets;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;
import org.apache.geode.internal.cache.persistence.PersistentStateListener;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class RegionAdvisor extends CacheDistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  /**
   * Number of threads allowed to concurrently volunteer for bucket primary.
   */
  public static final short VOLUNTEERING_THREAD_COUNT = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "RegionAdvisor.volunteeringThreadCount", 1)
      .shortValue();

  /**
   * Non-thread safe queue for volunteering for primary bucket. Each BucketAdvisor for this PR uses
   * this queue. The thread that uses this queue is a waiting pool thread. Any thread using this
   * queue must synchronize on this queue.
   */
  private final Queue volunteeringQueue = new ConcurrentLinkedQueue();

  /**
   * Semaphore with {@link #VOLUNTEERING_THREAD_COUNT} number of permits to control number of
   * threads volunteering for bucket primaries.
   */
  private final Semaphore volunteeringSemaphore = new Semaphore(VOLUNTEERING_THREAD_COUNT);

  private volatile int lastActiveProfiles = 0;
  private volatile int numDataStores = 0;
  protected volatile ProxyBucketRegion[] buckets;

  private Queue preInitQueue;
  private final Object preInitQueueMonitor = new Object();

  private ConcurrentHashMap<Integer, Set<ServerBucketProfile>> clientBucketProfilesMap;

  private RegionAdvisor(PartitionedRegion region) {
    super(region);
    synchronized (this.preInitQueueMonitor) {
      this.preInitQueue = new ConcurrentLinkedQueue();
    }
    this.clientBucketProfilesMap = new ConcurrentHashMap<Integer, Set<ServerBucketProfile>>();
  }

  public static RegionAdvisor createRegionAdvisor(PartitionedRegion region) {
    RegionAdvisor advisor = new RegionAdvisor(region);
    advisor.initialize();
    return advisor;
  }

  public PartitionedRegionStats getPartitionedRegionStats() {
    return getPartitionedRegion().getPrStats();
  }

  public synchronized void initializeRegionAdvisor() {
    if (this.buckets != null) {
      return;
    }
    PartitionedRegion p = getPartitionedRegion();
    int numBuckets = p.getAttributes().getPartitionAttributes().getTotalNumBuckets();
    ProxyBucketRegion[] bucs = new ProxyBucketRegion[numBuckets];

    InternalRegionArguments args = new InternalRegionArguments();
    args.setPartitionedRegionAdvisor(this);
    for (int i = 0; i < bucs.length; i++) {
      bucs[i] = new ProxyBucketRegion(i, p, args);
      bucs[i].initialize();
    }
    this.buckets = bucs;
  }

  /**
   * Process those profiles which were received during the initialization period. It is safe to
   * process these profiles potentially out of order due to the profiles version which is
   * established on the sender.
   */
  public void processProfilesQueuedDuringInitialization() {
    synchronized (this.preInitQueueMonitor) {
      Iterator pi = this.preInitQueue.iterator();
      boolean finishedInitQueue = false;
      try {
        while (pi.hasNext()) {
          Object o = pi.next();
          QueuedBucketProfile qbp = (QueuedBucketProfile) o;
          if (!qbp.isRemoval) {
            if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
              logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                  "applying queued profile addition for bucket {}", qbp.bucketId);
            }
            getBucket(qbp.bucketId).getBucketAdvisor().putProfile(qbp.bucketProfile);
          } else if (qbp.memberDeparted
              || !getDistributionManager().isCurrentMember(qbp.memberId)) {
            boolean crashed;
            if (qbp.memberDeparted) {
              crashed = qbp.crashed;
            } else {
              // TODO not necessarily accurate, but how important is this?
              crashed = !stillInView(qbp.memberId);
            }
            if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
              logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                  "applying queued member departure for all buckets for {}", qbp.memberId);
            }
            for (int i = 0; i < this.buckets.length; i++) {
              BucketAdvisor ba = this.buckets[i].getBucketAdvisor();
              ba.removeId(qbp.memberId, crashed, qbp.destroyed, qbp.fromMembershipListener);
            } // for
          } else { // apply removal for member still in the view
            if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
              logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                  "applying queued profile removal for all buckets for {}", qbp.memberId);
            }
            for (int i = 0; i < this.buckets.length; i++) {
              BucketAdvisor ba = this.buckets[i].getBucketAdvisor();
              int serial = qbp.serials[i];
              if (serial != ILLEGAL_SERIAL) {
                ba.removeIdWithSerial(qbp.memberId, serial, qbp.destroyed);
              }
            } // for
          } // apply removal for member still in the view
        } // while
        finishedInitQueue = true;
      } finally {
        this.preInitQueue = null; // prevent further additions to the queue
        this.preInitQueueMonitor.notifyAll();
        if (!finishedInitQueue && !getAdvisee().getCancelCriterion().isCancelInProgress()) {
          logger.error(LocalizedMessage.create(
              LocalizedStrings.RegionAdvisor_FAILED_TO_PROCESS_ALL_QUEUED_BUCKETPROFILES_FOR_0,
              getAdvisee()));
        }
      }
    }
  }

  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new PartitionProfile(memberId, version);
  }

  /**
   * Returns the {@link #volunteeringQueue} used to queue primary volunteering tasks by this PR's
   * BucketAdvisors.
   *
   * @return the volunteering queue for use by this PR's BucketAdvisors
   */
  public Queue getVolunteeringQueue() {
    return this.volunteeringQueue;
  }

  /**
   * Returns the {@link #volunteeringSemaphore} for controlling the number of threads that this PR's
   * BucketAdvisors are allowed to use for volunteering to be primary.
   *
   * @return the semaphore for controlling number of volunteering threads
   */
  public Semaphore getVolunteeringSemaphore() {
    return this.volunteeringSemaphore;
  }

  /**
   * Returns an unmodifiable map of bucket IDs to locations hosting the bucket.
   */
  public Map<Integer, List<BucketServerLocation66>> getAllClientBucketProfiles() {
    Map<Integer, List<BucketServerLocation66>> bucketToServerLocations =
        new HashMap<Integer, List<BucketServerLocation66>>();
    for (Integer bucketId : this.clientBucketProfilesMap.keySet()) {
      ArrayList<BucketServerLocation66> clientBucketProfiles =
          new ArrayList<BucketServerLocation66>();
      for (BucketProfile profile : this.clientBucketProfilesMap.get(bucketId)) {
        if (profile.isHosting) {
          ServerBucketProfile cProfile = (ServerBucketProfile) profile;
          Set<BucketServerLocation66> bucketServerLocations = cProfile.getBucketServerLocations();
          // Either we can make BucketServeLocation having ServerLocation with them
          // Or we can create bucketServerLocation as it is by iterating over the set of servers
          clientBucketProfiles.addAll(bucketServerLocations);
        }
      }
      bucketToServerLocations.put(bucketId, clientBucketProfiles);
    }

    if (getPartitionedRegion().isDataStore()) {
      for (Integer bucketId : getPartitionedRegion().getDataStore().getAllLocalBucketIds()) {
        BucketProfile profile = getBucketAdvisor(bucketId).getLocalProfile();

        if (logger.isDebugEnabled()) {
          logger.debug("The local profile is : {}", profile);
        }

        if (profile != null) {
          List<BucketServerLocation66> clientBucketProfiles = bucketToServerLocations.get(bucketId);
          if (clientBucketProfiles == null) {
            clientBucketProfiles = new ArrayList<BucketServerLocation66>();
            bucketToServerLocations.put(bucketId, clientBucketProfiles);
          }
          if ((profile instanceof ServerBucketProfile) && profile.isHosting) {
            ServerBucketProfile cProfile = (ServerBucketProfile) profile;
            Set<BucketServerLocation66> bucketServerLocations = cProfile.getBucketServerLocations();
            // Either we can make BucketServeLocation having ServerLocation with
            // them
            // Or we can create bucketServerLocation as it is by iterating over
            // the set of servers
            clientBucketProfiles.removeAll(bucketServerLocations);
            clientBucketProfiles.addAll(bucketServerLocations);
          }
        }
      }
    }
    return bucketToServerLocations;
  }

  public ConcurrentHashMap<Integer, Set<ServerBucketProfile>> getAllClientBucketProfilesTest() {
    ConcurrentHashMap<Integer, Set<ServerBucketProfile>> map =
        new ConcurrentHashMap<Integer, Set<ServerBucketProfile>>();
    Map<Integer, List<BucketServerLocation66>> testMap =
        new HashMap<>(this.getAllClientBucketProfiles());
    for (Integer bucketId : testMap.keySet()) {
      Set<ServerBucketProfile> parr = new HashSet<>(this.clientBucketProfilesMap.get(bucketId));
      map.put(bucketId, parr);
    }

    if (getPartitionedRegion().isDataStore()) {
      for (Integer bucketId : getPartitionedRegion().getDataStore().getAllLocalBucketIds()) {
        BucketProfile profile = getBucketAdvisor(bucketId).getLocalProfile();
        if ((profile instanceof ServerBucketProfile) && profile.isHosting) {
          map.get(bucketId).add((ServerBucketProfile) profile);
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("This maps is sksk {} and size is {}", map, map.keySet().size());
    }
    return map;
  }


  public Set<ServerBucketProfile> getClientBucketProfiles(Integer bucketId) {
    return this.clientBucketProfilesMap.get(bucketId);
  }

  public void setClientBucketProfiles(Integer bucketId, Set<ServerBucketProfile> profiles) {
    this.clientBucketProfilesMap.put(bucketId, Collections.unmodifiableSet(profiles));
  }

  /**
   * Close the bucket advisors, releasing any locks for primary buckets
   *
   * @return returns a list of primary bucket IDs
   *
   */
  public List closeBucketAdvisors() {
    List primariesHeld = Collections.EMPTY_LIST;
    if (this.buckets != null) {
      for (int i = 0; i < this.buckets.length; i++) {
        ProxyBucketRegion pbr = this.buckets[i];
        if (pbr.isPrimary()) {
          if (primariesHeld == Collections.EMPTY_LIST) {
            primariesHeld = new ArrayList();
          }
          primariesHeld.add(Integer.valueOf(i));
        }
        pbr.close();
      }
    }
    return primariesHeld;
  }

  /**
   * Close the adviser and all bucket advisors.
   */
  @Override
  public void close() {
    super.close();
    if (this.buckets != null) {
      for (int i = 0; i < this.buckets.length; i++) {
        this.buckets[i].close();
      }
    }
  }

  @Override
  public boolean removeId(ProfileId memberId, boolean crashed, boolean regionDestroyed,
      boolean fromMembershipListener) {
    // It's important that we remove member from the bucket advisors first
    // Calling super.removeId triggers redundancy satisfaction, so the bucket
    // advisors must have up to data information at that point.
    boolean removeBuckets = true;
    synchronized (this.preInitQueueMonitor) {
      if (this.preInitQueue != null) {
        // Queue profile during pre-initialization
        assert memberId instanceof InternalDistributedMember;
        QueuedBucketProfile qbf = new QueuedBucketProfile((InternalDistributedMember) memberId,
            crashed, regionDestroyed, fromMembershipListener);
        this.preInitQueue.add(qbf);
        removeBuckets = false;
      }
    } // synchronized
    if (removeBuckets && this.buckets != null) {
      for (int i = 0; i < this.buckets.length; i++) {
        ProxyBucketRegion pbr = this.buckets[i];
        BucketAdvisor pbra = pbr.getBucketAdvisor();

        boolean shouldSync = false;
        Profile profile = null;
        InternalDistributedMember mbr = null;
        if (memberId instanceof InternalDistributedMember) {
          mbr = (InternalDistributedMember) memberId;
          shouldSync = pbra.shouldSyncForCrashedMember(mbr);
          if (shouldSync) {
            profile = pbr.getBucketAdvisor().getProfile(memberId);
          }
        }
        boolean removed = pbr.getBucketAdvisor().removeId(memberId, crashed, regionDestroyed,
            fromMembershipListener);
        if (removed && shouldSync) {
          pbra.syncForCrashedMember(mbr, profile);
        }
      }
    }

    boolean removedId = false;
    removedId = super.removeId(memberId, crashed, regionDestroyed, fromMembershipListener);
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "RegionAdvisor#removeId: removing member from region {}: {}; removed = {}; crashed = {}",
          this.getPartitionedRegion().getName(), memberId, removedId, crashed);
    }

    return removedId;
  }

  /**
   * Clear the knowledge of given member from this advisor. In particular, clear the knowledge of
   * remote Bucket locations so that we avoid sending partition messages to buckets that will soon
   * be destroyed.
   *
   * @param memberId member that has closed the region
   * @param prSerial serial number of this partitioned region
   * @param serials serial numbers of buckets that need to be removed
   */
  public void removeIdAndBuckets(InternalDistributedMember memberId, int prSerial, int serials[],
      boolean regionDestroyed) {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "RegionAdvisor#removeIdAndBuckets: removing member from region {}: {}; buckets = ({}) serials",
          this.getPartitionedRegion().getName(), memberId,
          (serials == null ? "null" : serials.length));
    }

    synchronized (this.preInitQueueMonitor) {
      if (this.preInitQueue != null) {
        // Queue profile during pre-initialization
        QueuedBucketProfile qbf = new QueuedBucketProfile(memberId, serials, regionDestroyed);
        this.preInitQueue.add(qbf);
        return;
      }
    } // synchronized

    // OK, apply the update NOW
    if (this.buckets != null) {
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "RegionAdvisor#removeIdAndBuckets: removing buckets for member{};{}", memberId, this);
      }
      for (int i = 0; i < this.buckets.length; i++) {
        int s = serials[i];
        if (s != ILLEGAL_SERIAL) {
          if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
            logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                "RegionAdvisor#removeIdAndBuckets: removing bucket #{} serial {}", i, s);
          }
          this.buckets[i].getBucketAdvisor().removeIdWithSerial(memberId, s, regionDestroyed);
        }
      } // for

      super.removeIdWithSerial(memberId, prSerial, regionDestroyed);
    }
  }

  /**
   * Iterates over all buckets and marks them sick if the given member hosts the bucket.
   *
   * @param sick true if the bucket should be marked sick, false if healthy
   */
  public void markBucketsOnMember(DistributedMember member, boolean sick) {
    // The health profile exchange at cache level should take care of preInitQueue
    if (buckets == null) {
      return;
    }
    for (int i = 0; i < buckets.length; i++) {
      if (sick && !this.buckets[i].getBucketOwners().contains(member)) {
        continue;
      }
      this.buckets[i].setBucketSick(member, sick);
      if (logger.isDebugEnabled()) {
        logger.debug("Marked bucket ({}) {}", getPartitionedRegion().bucketStringForLogs(i),
            (this.buckets[i].isBucketSick() ? "sick" : "healthy"));
      }
    }
  }

  public void updateBucketStatus(int bucketId, DistributedMember member, boolean profileRemoved) {
    if (profileRemoved) {
      this.buckets[bucketId].setBucketSick(member, false);

    } else {
      ResourceAdvisor advisor = getPartitionedRegion().getCache().getResourceAdvisor();
      boolean sick = advisor.adviseCritialMembers().contains(member);
      if (logger.isDebugEnabled()) {
        logger.debug("updateBucketStatus:({}):member:{}:sick:{}",
            getPartitionedRegion().bucketStringForLogs(bucketId), member, sick);
      }
      this.buckets[bucketId].setBucketSick(member, sick);
    }
  }

  /**
   * throws LowMemoryException if the given bucket is hosted on a member which has crossed the
   * ResourceManager threshold.
   *
   * @param key for bucketId used in exception
   */
  public void checkIfBucketSick(final int bucketId, final Object key) throws LowMemoryException {
    if (MemoryThresholds.isLowMemoryExceptionDisabled()) {
      return;
    }
    assert this.buckets != null;
    assert this.buckets[bucketId] != null;
    if (this.buckets[bucketId].isBucketSick()) {
      Set<DistributedMember> sm = this.buckets[bucketId].getSickMembers();
      if (sm.isEmpty()) {
        // check again as this list is obtained under synchronization
        // fixes bug 50845
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("For bucket {} sick members are ",
            getPartitionedRegion().bucketStringForLogs(bucketId), sm);
      }
      throw new LowMemoryException(LocalizedStrings.ResourceManager_LOW_MEMORY_PR_0_KEY_1_MEMBERS_2
          .toLocalizedString(new Object[] {getPartitionedRegion().getFullPath(), key, sm}), sm);
    }
  }

  /**
   * Profile information for a remote counterpart.
   */
  public static class PartitionProfile extends CacheProfile {

    /**
     * The number of Mb the VM is allowed to use for the PR
     * {@link PartitionedRegion#getLocalMaxMemory()}
     */
    public int localMaxMemory;

    /**
     * A data store is a VM that has a non-zero local max memory, Since the localMaxMemory is
     * already sent, there is no need to send this state as it's implied in localMaxMemory
     */
    public transient boolean isDataStore = false;

    /**
     * requiresNotification determines whether a member needs to be notified of cache operations so
     * that cache listeners and other hooks can be engaged
     *
     * @since GemFire 5.1
     */
    public boolean requiresNotification = false;

    /**
     * A lock used to order operations that need to know about the imminent closure/destruction of a
     * Region
     */
    // private StoppableReentrantReadWriteLock isClosingLock = null;

    /**
     * Track the number of buckets this data store may have, implies isDataStore == true This value
     * is NOT sent directly but updated when {@link org.apache.geode.internal.cache.BucketAdvisor}s
     * receive updates
     */
    public transient short numBuckets = 0;

    /**
     * represents the list of the fixed partitions defined for this region.
     */
    public List<FixedPartitionAttributesImpl> fixedPAttrs;


    // Indicate the status of shutdown request
    public int shutDownAllStatus = PartitionedRegion.RUNNING_MODE;

    /** for internal use, required for DataSerializer.readObject */
    public PartitionProfile() {}

    public PartitionProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
      this.isPartitioned = true;
    }

    @Override
    protected int getIntInfo() {
      int s = super.getIntInfo();
      if (this.requiresNotification)
        s |= REQUIRES_NOTIFICATION_MASK;
      return s;
    }

    @Override
    protected void setIntInfo(int s) {
      super.setIntInfo(s);
      this.requiresNotification = (s & REQUIRES_NOTIFICATION_MASK) != 0;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.localMaxMemory = in.readInt();
      this.isDataStore = this.localMaxMemory > 0;
      this.fixedPAttrs = DataSerializer.readObject(in);
      this.shutDownAllStatus = in.readInt();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.localMaxMemory);
      DataSerializer.writeObject(this.fixedPAttrs, out);
      out.writeInt(this.shutDownAllStatus);
    }

    // public StoppableReentrantReadWriteLock.StoppableReadLock
    // getIsClosingReadLock(CancelCriterion stopper) {
    // synchronized (this) {
    // if (isClosingLock == null) {
    // this.isClosingLock = new StoppableReentrantReadWriteLock(stopper);
    // }
    // }
    // return this.isClosingLock.readLock();
    // }

    // public Lock getIsClosingWriteLock() {
    // return this.isClosingLock.writeLock();
    // }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("RegionAdvisor.PartitionProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; isDataStore=").append(this.isDataStore).append("; requiresNotification=")
          .append(this.requiresNotification).append("; localMaxMemory=").append(this.localMaxMemory)
          .append("; numBuckets=").append(this.numBuckets);
      if (this.fixedPAttrs != null) {
        sb.append("; FixedPartitionAttributes=").append(this.fixedPAttrs);
      }
      sb.append("; filterProfile=").append(this.filterProfile);
      sb.append("; shutDownAllStatus=").append(this.shutDownAllStatus);
    }

    @Override
    public int getDSFID() {
      return PARTITION_PROFILE;
    }


  } // end class PartitionProfile

  public int getNumDataStores() {
    final int numProfs = getNumProfiles();
    if (this.lastActiveProfiles != numProfs) {
      this.numDataStores = adviseDataStore().size();
      this.lastActiveProfiles = numProfs;
    }
    return this.numDataStores;
  }

  public Set<InternalDistributedMember> adviseDataStore() {
    return this.adviseDataStore(false);
  }

  /**
   * Returns the set of data stores that have finished initialization.
   */
  public Set<InternalDistributedMember> adviseInitializedDataStore() {
    Set<InternalDistributedMember> s = adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          return p.isDataStore && (!p.dataPolicy.withPersistence() || p.regionInitialized);
        }
        return false;
      }
    });
    return s;
  }

  /**
   * Returns the set of members that are not arrived at specified shutDownAll status
   */
  public Set<InternalDistributedMember> adviseNotAtShutDownAllStatus(final int status) {
    Set<InternalDistributedMember> s = adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          return p.isDataStore && p.shutDownAllStatus < status;
        }
        return false;
      }
    });
    return s;
  }

  public void waitForProfileStatus(int status) {
    ProfileShutdownListener listener = new ProfileShutdownListener();
    addProfileChangeListener(listener);
    try {
      int memberNum = 0;
      String regionName = getPartitionedRegion().getFullPath();
      do {
        Region pr = getPartitionedRegion().getCache().getRegion(regionName);
        if (pr == null || pr.isDestroyed())
          break;
        Set members = adviseNotAtShutDownAllStatus(status);
        memberNum = members.size();
        if (memberNum > 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("waitForProfileStatus {} at PR:{}, expecting {} members: {}", status,
                getPartitionedRegion().getFullPath(), memberNum, members);
          }
          listener.waitForChange();
        }
      } while (memberNum > 0);
    } finally {
      removeProfileChangeListener(listener);
    }
  }

  /**
   * Return a real Set if set to true, which can be modified
   *
   * @param realHashSet true if a real set is needed
   * @return depending on the realHashSet value may be a HashSet
   */
  public Set<InternalDistributedMember> adviseDataStore(boolean realHashSet) {
    Set<InternalDistributedMember> s = adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          return p.isDataStore;
        }
        return false;
      }
    });

    if (realHashSet) {
      if (s == Collections.EMPTY_SET) {
        s = new HashSet<InternalDistributedMember>();
      }
    }

    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "adviseDataStore returning {} from {}",
          s, toStringWithProfiles());
    }
    return s;
  }

  /**
   * return the set of the distributed members on which the given partition name is defined.
   *
   */

  public Set<InternalDistributedMember> adviseFixedPartitionDataStores(final String partitionName) {
    Set<InternalDistributedMember> s = adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are
        // Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          if (p.fixedPAttrs != null) {
            for (FixedPartitionAttributesImpl fpa : p.fixedPAttrs) {
              if (fpa.getPartitionName().equals(partitionName)) {
                return true;
              }
            }
          }
        }
        return false;
      }
    });

    if (s == Collections.EMPTY_SET) {
      s = new HashSet<InternalDistributedMember>();
    }

    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "adviseFixedPartitionDataStore returning {} from {}", s, toStringWithProfiles());
    }
    return s;
  }

  /**
   * return a distributed members on which the primary partition for given bucket is defined
   *
   */
  public InternalDistributedMember adviseFixedPrimaryPartitionDataStore(final int bucketId) {
    final List<InternalDistributedMember> fixedPartitionDataStore =
        new ArrayList<InternalDistributedMember>(1);
    fetchProfiles(new Filter() {
      public boolean include(Profile profile) {
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          if (p.fixedPAttrs != null) {
            for (FixedPartitionAttributesImpl fpa : p.fixedPAttrs) {
              if (fpa.isPrimary() && fpa.hasBucket(bucketId)) {
                fixedPartitionDataStore.add(0, p.getDistributedMember());
                return true;
              }
            }
          }
        }
        return false;
      }
    });

    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "adviseFixedPartitionDataStore returning {} from {}", fixedPartitionDataStore,
          toStringWithProfiles());
    }
    if (fixedPartitionDataStore.isEmpty()) {
      return null;
    }
    return fixedPartitionDataStore.get(0);
  }

  /**
   * Returns the list of all remote FixedPartitionAttributes defined across all members for the
   * given partitioned region
   *
   * @return list of all partitions(primary as well as secondary) defined on remote nodes
   */
  public List<FixedPartitionAttributesImpl> adviseAllFixedPartitionAttributes() {
    final List<FixedPartitionAttributesImpl> allFPAs =
        new ArrayList<FixedPartitionAttributesImpl>();
    fetchProfiles(new Filter() {
      public boolean include(final Profile profile) {
        if (profile instanceof PartitionProfile) {
          final PartitionProfile pp = (PartitionProfile) profile;
          if (pp.fixedPAttrs != null) {
            allFPAs.addAll(pp.fixedPAttrs);
            return true;
          }
        }
        return false;
      }
    });
    return allFPAs;
  }

  /**
   * Returns the list of all FixedPartitionAttributes defined across all members of given
   * partitioned region for a given FixedPartitionAttributes
   *
   * @return the list of same partitions defined on other nodes(can be primary or secondary)
   */
  public List<FixedPartitionAttributesImpl> adviseSameFPAs(final FixedPartitionAttributesImpl fpa) {
    final List<FixedPartitionAttributesImpl> sameFPAs =
        new ArrayList<FixedPartitionAttributesImpl>();

    fetchProfiles(new Filter() {
      public boolean include(final Profile profile) {
        if (profile instanceof PartitionProfile) {
          final PartitionProfile pp = (PartitionProfile) profile;
          List<FixedPartitionAttributesImpl> fpaList = pp.fixedPAttrs;
          if (fpaList != null) {
            int index = fpaList.indexOf(fpa);
            if (index != -1) {
              sameFPAs.add(fpaList.get(index));
            }
            return true;
          }
        }
        return false;
      }
    });
    return sameFPAs;
  }

  /**
   * Returns the list of all remote primary FixedPartitionAttributes defined across members for the
   * given partitioned region
   *
   * @return list of all primary partitions defined on remote nodes
   */
  public List<FixedPartitionAttributesImpl> adviseRemotePrimaryFPAs() {
    final List<FixedPartitionAttributesImpl> remotePrimaryFPAs =
        new ArrayList<FixedPartitionAttributesImpl>();

    fetchProfiles(new Filter() {
      public boolean include(final Profile profile) {
        if (profile instanceof PartitionProfile) {
          final PartitionProfile pp = (PartitionProfile) profile;
          List<FixedPartitionAttributesImpl> fpaList = pp.fixedPAttrs;
          if (fpaList != null) {
            for (FixedPartitionAttributesImpl fpa : fpaList) {
              if (fpa.isPrimary()) {
                remotePrimaryFPAs.add(fpa);
                return true;
              }
            }
          }
        }
        return false;
      }
    });
    return remotePrimaryFPAs;
  }

  /**
   * TODO remove this when Primary Bucket impl. is permanently in place
   *
   * @return the node??
   */
  public Node adviseSmallestDataStore(final List limitNodeList) {
    final HashMap filtSet = new HashMap(limitNodeList.size());
    Node n = null;
    for (Iterator filtI = limitNodeList.iterator(); filtI.hasNext();) {
      n = (Node) filtI.next();
      filtSet.put(n.getMemberId(), n);
    }
    final Object[] smallest = new Object[1];
    adviseFilter(new Filter() {
      short numBucks = Short.MAX_VALUE;

      public boolean include(Profile profile) {
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          if (filtSet.containsKey(p.getDistributedMember()) && p.numBuckets < this.numBucks) {
            smallest[0] = p.getDistributedMember();
            this.numBucks = p.numBuckets;
          }
        }
        return false;
      }
    });
    return (Node) filtSet.get(smallest[0]);
  }


  public List<DistributedMember> orderDataStoresUsingBucketCount(final Set nodes) {
    final Set<NodeBucketSize> orderedSet = new TreeSet<NodeBucketSize>();
    final List<DistributedMember> orderedList = new ArrayList<DistributedMember>();
    final DistributedMember self = getDistributionManager().getDistributionManagerId();
    adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        if (profile instanceof PartitionProfile && nodes.contains(profile.getDistributedMember())) {
          PartitionProfile p = (PartitionProfile) profile;
          orderedSet.add(new NodeBucketSize(p.numBuckets, p.getDistributedMember()));
          return true;
        } else if (profile instanceof PartitionProfile && nodes.contains(self)) {
          orderedSet.add(new NodeBucketSize(getBucketSet().size(), self));
          return true;
        }
        return false;
      }
    });

    if (nodes.contains(self)
        && !orderedSet.contains(new NodeBucketSize(getBucketSet().size(), self))) {
      orderedSet.add(new NodeBucketSize(getBucketSet().size(), self));
    }
    for (NodeBucketSize node : orderedSet) {
      orderedList.add(node.member);
    }
    return orderedList;
  }

  private class NodeBucketSize implements Comparable {
    private final int numBuckets;

    private final DistributedMember member;

    public NodeBucketSize(final int numBuckets, final DistributedMember member) {
      this.numBuckets = numBuckets;
      this.member = member;
    }

    public int compareTo(Object o) {
      assert o instanceof NodeBucketSize;
      NodeBucketSize node = (NodeBucketSize) o;
      if (node.numBuckets > this.numBuckets) {
        return 1;
      }
      return -1;
    }

    @Override
    public String toString() {
      return "NodeBucketSize [ member =" + member + " numBuckets = " + numBuckets + "]";
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NodeBucketSize)) {
        return false;
      }
      NodeBucketSize node = (NodeBucketSize) obj;
      if (this.member.getId().equals(node.member.getId())) {
        return true;
      }
      return false;
    }
  }

  public Set adviseAllPRNodes() {
    return adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        CacheProfile prof = (CacheProfile) profile;
        return prof.isPartitioned;
      }
    });
  }

  public Set adviseAllServersWithInterest() {
    return adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        CacheProfile prof = (CacheProfile) profile;
        return prof.hasCacheServer && prof.filterProfile != null
            && prof.filterProfile.hasInterest();
      }
    });
  }

  private static final Filter prServerWithInterestFilter = new Filter() {
    public boolean include(Profile profile) {
      CacheProfile prof = (CacheProfile) profile;
      return prof.isPartitioned && prof.hasCacheServer && prof.filterProfile != null
          && prof.filterProfile.hasInterest();
    }
  };

  public boolean hasPRServerWithInterest() {
    return satisfiesFilter(prServerWithInterestFilter);
  }

  /**
   * return the set of all members who must receive operation notifications
   *
   * @since GemFire 5.1
   */
  public Set adviseRequiresNotification(final EntryEventImpl event) {
    return adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        if (profile instanceof PartitionProfile) {
          PartitionProfile prof = (PartitionProfile) profile;
          if (prof.isPartitioned) {
            if (prof.hasCacheListener) {
              InterestPolicy pol = prof.subscriptionAttributes.getInterestPolicy();
              if (pol == InterestPolicy.ALL) {
                return true;
              }
            }
            if (prof.requiresNotification) {
              return true;
            }
            return false;
          }
        }
        return false;
      }
    });
  }

  @Override
  public synchronized boolean putProfile(Profile p) {
    assert p instanceof CacheProfile;
    CacheProfile profile = (CacheProfile) p;
    PartitionedRegion pr = getPartitionedRegion();
    if (profile.hasCacheLoader) {
      pr.setHaveCacheLoader();
    }
    // don't keep FilterProfiles around in accessors. They're needed only for
    // routing messages in data stors
    if (profile.filterProfile != null) {
      if (!pr.isDataStore()) {
        profile.filterProfile = null;
      }
    }
    return super.putProfile(profile);
  }

  public PartitionProfile getPartitionProfile(InternalDistributedMember id) {
    return (PartitionProfile) getProfile(id);
  }

  public boolean isPrimaryForBucket(int bucketId) {
    if (this.buckets == null) {
      return false;
    }
    return this.buckets[bucketId].isPrimary();
  }

  /**
   * Returns true if the bucket is currently being hosted locally. Note that as soon as this call
   * returns, this datastore may begin to host the bucket, thus two calls in a row may be different.
   *
   * @param bucketId the index of the bucket to check
   * @return true if the bucket is currently being hosted locally
   */
  public boolean isBucketLocal(int bucketId) {
    if (this.buckets == null) {
      return false;
    }
    return this.buckets[bucketId].getHostedBucketRegion() != null;
  }

  public boolean areBucketsInitialized() {
    return this.buckets != null;
  }

  /**
   * Returns the real BucketRegion if it's currently locally hosted. Otherwise the ProxyBucketRegion
   * is returned. Note that this member may be in the process of hosting the real bucket. Until that
   * has completed, getBucket will continue to return the ProxyBucketRegion.
   *
   * @param bucketId the index of the bucket to retrieve
   * @return the bucket identified by bucketId
   */
  public Bucket getBucket(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    ProxyBucketRegion pbr = this.buckets[bucketId];
    Bucket ret = pbr.getHostedBucketRegion();
    if (ret != null) {
      return ret;
    } else {
      return pbr;
    }
  }

  /**
   * Returns the BucketAdvisor for the specified bucket.
   *
   * @param bucketId the index of the bucket to retrieve the advisor for
   * @return the bucket advisor identified by bucketId
   */
  public BucketAdvisor getBucketAdvisor(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    ProxyBucketRegion pbr = this.buckets[bucketId];
    Bucket ret = pbr.getHostedBucketRegion();
    if (ret != null) {
      return ret.getBucketAdvisor();
    } else {
      return pbr.getBucketAdvisor();
    }
  }

  public Map<Integer, BucketAdvisor> getAllBucketAdvisors() {
    Assert.assertTrue(this.buckets != null);
    Map<Integer, BucketAdvisor> map = new HashMap<Integer, BucketAdvisor>();
    for (int i = 0; i < buckets.length; i++) {
      ProxyBucketRegion pbr = this.buckets[i];
      Bucket ret = pbr.getHostedBucketRegion();
      if (ret != null) {
        map.put(ret.getId(), ret.getBucketAdvisor());
      }
    }
    return map;
  }

  /**
   *
   * @return array of serial numbers for buckets created locally
   */
  public int[] getBucketSerials() {
    if (this.buckets == null) {
      return new int[0];
    }
    int result[] = new int[this.buckets.length];

    for (int i = 0; i < result.length; i++) {
      ProxyBucketRegion pbr = this.buckets[i];
      Bucket b = pbr.getCreatedBucketRegion();
      if (b == null) {
        result[i] = ILLEGAL_SERIAL;
      } else {
        result[i] = b.getSerialNumber();
      }
    }
    return result;
  }

  /**
   * Returns the bucket identified by bucketId after waiting for initialization to finish processing
   * queued profiles. Call synchronizes and waits on {@link #preInitQueueMonitor}.
   *
   * @param bucketId the bucket identifier
   * @return the bucket identified by bucketId
   * @throws org.apache.geode.distributed.DistributedSystemDisconnectedException if interrupted for
   *         shutdown cancellation
   */
  public Bucket getBucketPostInit(int bucketId) {
    synchronized (this.preInitQueueMonitor) {
      boolean interrupted = false;
      try {
        while (this.preInitQueue != null) {
          try {
            this.preInitQueueMonitor.wait(); // spurious wakeup ok
          } catch (InterruptedException e) {
            interrupted = true;
            this.getAdvisee().getCancelCriterion().checkCancelInProgress(e);
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return getBucket(bucketId);
  }

  /**
   * Get the most recent primary node for the bucketId. Returns null if no primary can be found
   * within {@link DistributionConfig#getMemberTimeout}.
   *
   * @return the Node managing the primary copy of the bucket
   */
  public InternalDistributedMember getPrimaryMemberForBucket(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    Bucket b = this.buckets[bucketId];
    return b.getBucketAdvisor().getPrimary();
  }

  /**
   * Return the node favored for reading for the given bucket
   *
   * @param bucketId the bucket we want to read
   * @return the member, possibly null if no member is available
   */
  public InternalDistributedMember getPreferredNode(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    Bucket b = this.buckets[bucketId];
    return b.getBucketAdvisor().getPreferredNode();
  }

  public boolean isStorageAssignedForBucket(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    return this.buckets[bucketId].getBucketRedundancy() >= 0;
  }

  /**
   * @param bucketId the bucket to check redundancy on
   * @param minRedundancy the amount of expected redundancy; ignored if wait is false
   * @param wait true if caller wants us to wait for redundancy
   * @return true if redundancy on given bucket is detected
   */
  public boolean isStorageAssignedForBucket(int bucketId, int minRedundancy, boolean wait) {
    if (!wait) {
      return isStorageAssignedForBucket(bucketId);
    } else {
      Assert.assertTrue(this.buckets != null);
      return this.buckets[bucketId].getBucketAdvisor().waitForRedundancy(minRedundancy);
    }
  }

  public boolean waitForLocalBucketStorage(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    return this.buckets[bucketId].getBucketAdvisor().waitForStorage();
  }

  /**
   * Get the redundancy of the this bucket, taking into account the local bucket, if any.
   *
   * @return number of redundant copies for a given bucket, or -1 if there are no instances of the
   *         bucket.
   */
  public int getBucketRedundancy(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    return this.buckets[bucketId].getBucketRedundancy();
  }

  /**
   * Return the set of all members who currently own the bucket, including the local owner, if
   * applicable
   *
   * @return a set of {@link InternalDistributedMember}s that own the bucket
   */
  public Set<InternalDistributedMember> getBucketOwners(int bucketId) {
    Assert.assertTrue(this.buckets != null);
    return this.buckets[bucketId].getBucketOwners();
  }

  /**
   * Return the set of buckets which have storage assigned
   *
   * @return set of Integer bucketIds
   */
  public Set<Integer> getBucketSet() {
    Assert.assertTrue(this.buckets != null);
    return new BucketSet();
  }


  public ProxyBucketRegion[] getProxyBucketArray() {
    return this.buckets;
  }

  private class BucketSet extends AbstractSet {
    final ProxyBucketRegion[] pbrs;

    public BucketSet() {
      this.pbrs = RegionAdvisor.this.buckets;
      Assert.assertTrue(this.pbrs != null);
    }

    @Override
    public Object[] toArray() {
      // A somewhat wasteful impl. but required because the size is not fixed
      ArrayList ar = new ArrayList(this.pbrs.length);
      try {
        for (Iterator e = iterator(); e.hasNext();) {
          ar.add(e.next());
        }
      } catch (NoSuchElementException allDone) {
      }
      return ar.toArray();
    }

    @Override
    public Object[] toArray(Object p_a[]) {
      Object a[] = p_a;
      // Some what wasteful, but needed because size is not fixed
      Object[] oa = toArray();

      if (a.length < oa.length) {
        a = (Object[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(),
            oa.length);
        System.arraycopy(oa, 0, a, 0, oa.length);
      }

      for (int i = 0; i < oa.length; i++)
        a[i] = oa[i];

      if (a.length > oa.length)
        a[oa.length] = null;

      return a;
    }


    /*
     * Note: The consistency between size(), hasNext() and next() is weak, meaning that the state of
     * the backing Set may change causing more or less elements to be available after calling size()
     */
    @Override
    public int size() {
      return this.pbrs.length;
    }

    @Override
    public Iterator iterator() {
      return new BucketSetIterator();
    }

    class BucketSetIterator implements Iterator {
      private int currentItem = -1;


      public void remove() {
        throw new UnsupportedOperationException();
      }

      /*
       * Note: The consistency guarantee between hasNext() and next() is weak. It's possible
       * hasNext() will return true and a following call to next() may throw NoSuchElementException
       * (due to loss of bucket storage). Its also equally possible for hasNext() to return false
       * and a subsequent call to next() will return a valid bucketid.
       */
      public boolean hasNext() {
        if (getPartitionedRegion().isFixedPartitionedRegion()) {
          if (this.currentItem + 1 < BucketSet.this.pbrs.length) {
            int possibleBucketId = this.currentItem;
            boolean bucketExists = false;

            List<FixedPartitionAttributesImpl> fpaList = adviseAllFixedPartitionAttributes();
            List<FixedPartitionAttributesImpl> localFpas =
                getPartitionedRegion().getFixedPartitionAttributesImpl();
            if (localFpas != null) {
              fpaList.addAll(localFpas);
            }
            while (++possibleBucketId < BucketSet.this.pbrs.length && !bucketExists) {
              for (FixedPartitionAttributesImpl fpa : fpaList) {
                if (fpa.hasBucket(possibleBucketId)) {
                  bucketExists = true;
                  break;
                }
              }
            }
            return bucketExists;
          } else {
            return false;
          }
        } else {
          return this.currentItem + 1 < BucketSet.this.pbrs.length;
        }
      }

      public Object next() {
        if (++this.currentItem < BucketSet.this.pbrs.length) {
          if (isStorageAssignedForBucket(this.currentItem)) {
            return Integer.valueOf(this.currentItem);
          } else {
            if (getPartitionedRegion().isFixedPartitionedRegion()) {
              boolean bucketExists = false;
              List<FixedPartitionAttributesImpl> fpaList = adviseAllFixedPartitionAttributes();
              List<FixedPartitionAttributesImpl> localFpas =
                  getPartitionedRegion().getFixedPartitionAttributesImpl();
              if (localFpas != null) {
                fpaList.addAll(localFpas);
              }
              do {
                for (FixedPartitionAttributesImpl fpa : fpaList) {
                  if (fpa.hasBucket(this.currentItem)) {
                    bucketExists = true;
                    break;
                  }
                }
                if (!bucketExists) {
                  this.currentItem++;
                }
              } while (this.currentItem < BucketSet.this.pbrs.length && !bucketExists);

              if (bucketExists) {
                getPartitionedRegion().createBucket(this.currentItem, 0, null);
                return Integer.valueOf(this.currentItem);
              }
            } else {
              getPartitionedRegion().createBucket(this.currentItem, 0, null);
              return Integer.valueOf(this.currentItem);
            }
          }
        }
        throw new NoSuchElementException();
      }
    }

  }

  /**
   * Obtain the ordered {@link ArrayList} of data stores limited to those specified in the provided
   * memberFilter.
   *
   * @param memberFilter the set of members allowed to be in the list.
   * @return a list of DataStoreBuckets
   */
  public ArrayList<DataStoreBuckets> adviseFilteredDataStores(
      final Set<InternalDistributedMember> memberFilter) {
    final HashMap<InternalDistributedMember, Integer> memberToPrimaryCount =
        new HashMap<InternalDistributedMember, Integer>();
    for (int i = 0; i < this.buckets.length; i++) {
      ProxyBucketRegion pbr = this.buckets[i];
      // quick dirty check
      InternalDistributedMember p = pbr.getBucketAdvisor().basicGetPrimaryMember();
      if (p != null) {
        Integer count = memberToPrimaryCount.get(p);
        if (count != null) {
          memberToPrimaryCount.put(p, Integer.valueOf(count.intValue() + 1));
        } else {
          memberToPrimaryCount.put(p, Integer.valueOf(1));
        }
      }
    }

    final ArrayList<DataStoreBuckets> ds = new ArrayList<DataStoreBuckets>(memberFilter.size());
    adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          if (memberFilter.contains(p.getDistributedMember())) {
            Integer priCount = memberToPrimaryCount.get(p.getDistributedMember());
            int primaryCount = 0;
            if (priCount != null) {
              primaryCount = priCount.intValue();
            }
            ds.add(new DataStoreBuckets(p.getDistributedMember(), p.numBuckets, primaryCount,
                p.localMaxMemory));
          }
        }
        return false;
      }
    });


    return ds;
  }

  public void incrementBucketCount(Profile p) {
    PartitionProfile pp = (PartitionProfile) getProfile(p.getDistributedMember());
    if (pp != null) {
      Assert.assertTrue(pp.isDataStore);
      pp.numBuckets++;
    }
  }

  public void decrementsBucketCount(Profile p) {
    PartitionProfile pp = (PartitionProfile) getProfile(p.getDistributedMember());
    if (pp != null) {
      Assert.assertTrue(pp.isDataStore);
      pp.numBuckets--;
      if (pp.numBuckets < 0) {
        pp.numBuckets = 0;
      }
    }
  }

  /**
   * Dumps out all profiles in this advisor AND all buckets. Callers should check for debug enabled.
   *
   * @param infoMsg prefix message to log
   */
  @Override
  public void dumpProfiles(String infoMsg) {
    if (logger.isDebugEnabled()) {
      logger.debug("[dumpProfiles] dumping {}", this.toStringWithProfiles());
    }

    // 1st dump all profiles for this RegionAdvisor
    super.dumpProfiles(infoMsg);

    // 2nd dump all profiles for each BucketAdvisor
    ProxyBucketRegion[] pbrs = this.buckets;
    if (pbrs == null) {
      return;
    }
    for (int i = 0; i < pbrs.length; i++) {
      pbrs[i].getBucketAdvisor().dumpProfiles(infoMsg);
      BucketPersistenceAdvisor persistentAdvisor = pbrs[i].getPersistenceAdvisor();
      if (persistentAdvisor != null) {
        persistentAdvisor.dump(infoMsg);
      }
    }
  }

  public void notPrimary(int bucketId, InternalDistributedMember wasPrimary) {
    Assert.assertTrue(this.buckets != null);
    ProxyBucketRegion b = this.buckets[bucketId];
    b.getBucketAdvisor().notPrimary(wasPrimary);
  }

  /**
   * Find the set of members which own primary buckets, including the local member
   *
   * @return set of InternalDistributedMember ids
   */
  public Set advisePrimaryOwners() {
    Assert.assertTrue(this.buckets != null);
    ProxyBucketRegion[] bucs = this.buckets;
    HashSet hs = new HashSet();
    for (int i = 0; i < bucs.length; i++) {
      if (isStorageAssignedForBucket(i)) {
        InternalDistributedMember mem = bucs[i].getBucketAdvisor().getPrimary();
        if (mem != null) {
          hs.add(mem);
        }
      }
    }
    return hs;
  }

  /**
   * A visitor interface for the buckets of this region used by
   * {@link RegionAdvisor#accept(BucketVisitor, Object)}.
   */
  public interface BucketVisitor<T> {

    /**
     * Visit a given {@link ProxyBucketRegion} accumulating the results in the given aggregate.
     * Returns false when the visit has to be terminated.
     */
    boolean visit(RegionAdvisor advisor, ProxyBucketRegion pbr, T aggregate);
  }

  /**
   * Invoke the given {@link BucketVisitor} on all the {@link ProxyBucketRegion} s exiting when the
   * {@link BucketVisitor#visit} method returns false.
   *
   * @param <T> the type of object used for aggregation of results
   * @param visitor the {@link BucketVisitor} to use for the visit
   * @param aggregate an aggregate object that will be used to for aggregation of results by the
   *        {@link BucketVisitor#visit} method; this allows the {@link BucketVisitor} to not
   *        maintain any state so that in most situations a global static object encapsulating the
   *        required behaviour will work
   *
   * @return true when the full visit completed, and false if it was terminated due to
   *         {@link BucketVisitor#visit} returning false
   */
  public <T> boolean accept(BucketVisitor<T> visitor, T aggregate) {
    final ProxyBucketRegion[] bucs = this.buckets;
    Assert.assertTrue(bucs != null);
    for (ProxyBucketRegion pbr : bucs) {
      if (!visitor.visit(this, pbr, aggregate)) {
        return false;
      }
    }
    return true;
  }

  public PartitionedRegion getPartitionedRegion() {
    return (PartitionedRegion) getAdvisee();
  }

  /**
   * Update or create a bucket's meta-data If this advisor has not completed initialization, upon
   * return the profile will be enqueued for processing during initialization, otherwise the profile
   * will be immediately processed. This architecture limits the blockage of threads during
   * initialization.
   *
   * @param bucketId the unique identifier of the bucket
   * @param profile the bucket meta-data from a particular member with the bucket
   */
  public void putBucketProfile(int bucketId, BucketProfile profile) {
    synchronized (this.preInitQueueMonitor) {
      if (this.preInitQueue != null) {
        // Queue profile during pre-initialization
        QueuedBucketProfile qbf = new QueuedBucketProfile(bucketId, profile);
        this.preInitQueue.add(qbf);
        return;
      }
    }

    // Directly process profile post-initialization
    getBucket(bucketId).getBucketAdvisor().putProfile(profile);
  }

  static class QueuedBucketProfile {
    protected final int bucketId;
    protected final BucketProfile bucketProfile;

    /** true means that this member has departed the view */
    protected final boolean memberDeparted;

    /** true means that this profile needs to be removed */
    protected final boolean isRemoval;

    /** true means that the peer crashed */
    protected final boolean crashed;

    /**
     * true means that this QueuedBucketProfile was created because of MembershipListener invocation
     */
    protected final boolean fromMembershipListener;

    protected final boolean destroyed;

    protected final InternalDistributedMember memberId;
    protected final int serials[];

    /**
     * Queue up an addition
     *
     * @param bId the bucket being added
     * @param p the profile to add
     */
    public QueuedBucketProfile(int bId, BucketProfile p) {
      this.bucketId = bId;
      this.bucketProfile = p;
      this.isRemoval = false;
      this.crashed = false;
      this.memberDeparted = false;
      this.memberId = null;
      this.serials = null;
      this.destroyed = false;
      this.fromMembershipListener = false;
    }

    /**
     * Queue up a removal due to member leaving the view
     *
     * @param mbr the member being removed
     */
    public QueuedBucketProfile(InternalDistributedMember mbr, boolean crashed, boolean destroyed,
        boolean fromMembershipListener) {
      this.bucketId = 0;
      this.bucketProfile = null;
      this.isRemoval = true;
      this.crashed = crashed;
      this.memberDeparted = true;
      this.memberId = mbr;
      this.serials = null;
      this.destroyed = destroyed;
      this.fromMembershipListener = fromMembershipListener;
    }

    /**
     * Queue up a removal due to region destroy
     *
     * @param mbr the member being removed
     * @param serials the serials it had
     */
    public QueuedBucketProfile(InternalDistributedMember mbr, int serials[], boolean destroyed) {
      this.bucketId = 0;
      this.bucketProfile = null;
      this.isRemoval = true;
      this.crashed = false;
      this.memberDeparted = false;
      this.memberId = mbr;
      this.serials = serials;
      this.destroyed = destroyed;
      this.fromMembershipListener = false;
    }
  }

  public Set adviseBucketProfileExchange() {
    return adviseDataStore();
  }

  public long adviseTotalMemoryAllocation() {
    final AtomicLong total = new AtomicLong();
    adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          total.addAndGet(p.localMaxMemory);
        }
        return false;
      }
    });
    return total.get();
  }

  public long adviseTotalMemoryAllocationForFPR() {
    final AtomicLong total = new AtomicLong();
    adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        // probably not needed as all profiles for a partitioned region are Partition profiles
        if (profile instanceof PartitionProfile) {
          PartitionProfile p = (PartitionProfile) profile;
          if (p.fixedPAttrs != null) {
            total.addAndGet(p.localMaxMemory);
          }
        }
        return false;
      }
    });
    return total.get();
  }

  /**
   * Returns true if there are any buckets created anywhere in the distributed system for this
   * partitioned region.
   */
  public boolean hasCreatedBuckets() {
    final ProxyBucketRegion[] bucs = this.buckets;
    if (bucs != null) {
      for (int i = 0; i < bucs.length; i++) {
        if (bucs[i].getBucketOwnersCount() > 0) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the total number of buckets created anywhere in the distributed system for this
   * partitioned region.
   *
   * @return the total number of buckets created anywhere for this PR
   */
  public int getCreatedBucketsCount() {
    final ProxyBucketRegion[] bucs = this.buckets;
    if (bucs == null) {
      return 0;
    }
    int createdBucketsCount = 0;
    for (int i = 0; i < bucs.length; i++) {
      if (bucs[i].getBucketOwnersCount() > 0) {
        createdBucketsCount++;
      }
    }
    return createdBucketsCount;
  }

  /**
   * Returns a possibly null list of this advisor's real bucket profiles. A real bucket profile is
   * one that for a bucket that actually has storage in this vm.
   *
   * @return a list of BucketProfileAndId instances; may be null
   * @since GemFire 5.5
   */
  public ArrayList getBucketRegionProfiles() {
    final ProxyBucketRegion[] bucs = this.buckets;
    if (bucs == null) {
      return null;
    }
    ArrayList result = new ArrayList(bucs.length);
    for (int i = 0; i < bucs.length; i++) {
      // Fix for 41436 - we need to include buckets that are still initializing here
      // we must start including buckets in this list *before* those buckets exchange
      // profiles.
      BucketRegion br = bucs[i].getCreatedBucketRegion();
      if (br != null) {
        result.add(new BucketProfileAndId(br.getProfile(), i));
      }
    }
    if (result.size() == 0) {
      result = null;
    }
    return result;
  }

  /**
   * Takes a list of BucketProfileAndId and adds them to thsi advisors proxy buckets.
   *
   * @since GemFire 5.5
   */
  public void putBucketRegionProfiles(ArrayList l) {
    int size = l.size();
    for (int i = 0; i < size; i++) {
      BucketProfileAndId bp = (BucketProfileAndId) l.get(i);
      int id = bp.getId();
      getBucket(id).getBucketAdvisor().putProfile(bp.getBucketProfile());
    }
  }

  /**
   * return true if the given member has this advisor's partitioned region
   */
  public boolean hasPartitionedRegion(InternalDistributedMember profileId) {
    if (getDistributionManager().getId().equals(profileId)) {
      return true;
    }
    return (getProfile(profileId) != null);
  }

  @Override
  protected void profileRemoved(Profile profile) {
    if (logger.isDebugEnabled()) {
      logger.debug("RA: removing profile {}", profile);
    }
    if (getAdvisee() instanceof PartitionedRegion) {
      ((PartitionedRegion) getAdvisee()).removeMemberFromCriticalList(profile.peerMemberId);
    }

    if (this.buckets != null) {
      for (int i = 0; i < this.buckets.length; i++) {
        this.buckets[i].getBucketAdvisor().checkForLostPrimaryElector(profile);
      }
    }
  }

  public void addPersistenceListener(PersistentStateListener listener) {
    for (int i = 0; i < buckets.length; i++) {
      PersistenceAdvisor advisor = buckets[i].getPersistenceAdvisor();
      if (advisor != null) {
        advisor.addListener(listener);
      }
    }
  }

  public static class BucketProfileAndId implements DataSerializable {
    private static final long serialVersionUID = 332892607792421553L;
    /* final */ private int id;
    // bid = bucket id
    /* final */ private BucketProfile bp;
    private boolean isServerBucketProfile = false;

    public BucketProfileAndId(Profile bp, int id) {
      this.id = id;
      this.bp = (BucketProfile) bp;
      if (bp instanceof ServerBucketProfile)
        isServerBucketProfile = true;
    }

    public BucketProfileAndId() {}

    public int getId() {
      return this.id;
    }

    public BucketProfile getBucketProfile() {
      return this.bp;
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.id = in.readInt();
      this.isServerBucketProfile = in.readBoolean();
      if (this.isServerBucketProfile)
        this.bp = new ServerBucketProfile();
      else
        this.bp = new BucketProfile();

      InternalDataSerializer.invokeFromData(this.bp, in);
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(this.id);
      out.writeBoolean(this.isServerBucketProfile);
      InternalDataSerializer.invokeToData(this.bp, out);
    }

    @Override
    public String toString() {
      return "BucketProfileAndId (profile=" + bp + "; id=" + id + ")";
    }
  }

  // profile listener to monitor remote member unexpected leave during shutdownAll
  private class ProfileShutdownListener implements ProfileListener {

    ProfileShutdownListener() {

    }

    private boolean profileChanged = false;

    public void waitForChange() {
      Region pr = getPartitionedRegion();

      synchronized (this) {
        while (!profileChanged && pr != null && !pr.isDestroyed()) {
          // the advisee might have been destroyed due to initialization failure
          try {
            this.wait(1000);
          } catch (InterruptedException e) {
          }
        }
        this.profileChanged = false;
      }
    }

    public void profileCreated(Profile profile) {
      profileUpdated(profile);
    }

    public void profileRemoved(Profile profile, boolean regionDestroyed) {
      // if a profile is gone, notify
      synchronized (this) {
        this.profileChanged = true;
        this.notifyAll();
      }
    }

    public void profileUpdated(Profile profile) {
      // when updated, notify the loop in GFC to check the list again
      synchronized (this) {
        this.profileChanged = true;
        this.notifyAll();
      }
    }
  }
}
