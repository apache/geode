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
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ProfileListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.BucketPersistenceAdvisor;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public class RegionAdvisor extends CacheDistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  /**
   * Number of threads allowed to concurrently volunteer for bucket primary.
   */
  public static final short VOLUNTEERING_THREAD_COUNT = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "RegionAdvisor.volunteeringThreadCount", 1)
      .shortValue();

  /**
   * Non-thread safe queue for volunteering for primary bucket. Each BucketAdvisor for this PR uses
   * this queue. The thread that uses this queue is a waiting pool thread. Any thread using this
   * queue must synchronize on this queue.
   */
  private final Queue<Runnable> volunteeringQueue = new ConcurrentLinkedQueue<>();

  /**
   * Semaphore with {@link #VOLUNTEERING_THREAD_COUNT} number of permits to control number of
   * threads volunteering for bucket primaries.
   */
  private final Semaphore volunteeringSemaphore = new Semaphore(VOLUNTEERING_THREAD_COUNT);

  private volatile int lastActiveProfiles = 0;
  private volatile int numDataStores = 0;
  protected volatile ProxyBucketRegion[] buckets;

  private Queue<QueuedBucketProfile> preInitQueue;
  private final Object preInitQueueMonitor = new Object();

  private ConcurrentHashMap<Integer, Set<ServerBucketProfile>> clientBucketProfilesMap;

  private RegionAdvisor(PartitionedRegion region) {
    super(region);
    synchronized (preInitQueueMonitor) {
      preInitQueue = new ConcurrentLinkedQueue<>();
    }
    clientBucketProfilesMap = new ConcurrentHashMap<>();
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
    if (buckets != null) {
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
    buckets = bucs;
  }

  /**
   * Process those profiles which were received during the initialization period. It is safe to
   * process these profiles potentially out of order due to the profiles version which is
   * established on the sender.
   */
  public void processProfilesQueuedDuringInitialization() {
    synchronized (preInitQueueMonitor) {
      Iterator pi = preInitQueue.iterator();
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
            for (ProxyBucketRegion bucket : buckets) {
              BucketAdvisor ba = bucket.getBucketAdvisor();
              ba.removeId(qbp.memberId, crashed, qbp.destroyed, qbp.fromMembershipListener);
            } // for
          } else { // apply removal for member still in the view
            if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
              logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                  "applying queued profile removal for all buckets for {}", qbp.memberId);
            }
            for (int i = 0; i < buckets.length; i++) {
              BucketAdvisor ba = buckets[i].getBucketAdvisor();
              int serial = qbp.serials[i];
              if (serial != ILLEGAL_SERIAL) {
                ba.removeIdWithSerial(qbp.memberId, serial, qbp.destroyed);
              }
            } // for
          } // apply removal for member still in the view
        } // while
        finishedInitQueue = true;
      } finally {
        preInitQueue = null; // prevent further additions to the queue
        preInitQueueMonitor.notifyAll();
        if (!finishedInitQueue && !getAdvisee().getCancelCriterion().isCancelInProgress()) {
          logger.error("Failed to process all queued BucketProfiles for {}",
              getAdvisee());
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
  public Queue<Runnable> getVolunteeringQueue() {
    return volunteeringQueue;
  }

  /**
   * Returns the {@link #volunteeringSemaphore} for controlling the number of threads that this PR's
   * BucketAdvisors are allowed to use for volunteering to be primary.
   *
   * @return the semaphore for controlling number of volunteering threads
   */
  public Semaphore getVolunteeringSemaphore() {
    return volunteeringSemaphore;
  }

  /**
   * Returns an unmodifiable map of bucket IDs to locations hosting the bucket.
   */
  public Map<Integer, List<BucketServerLocation66>> getAllClientBucketProfiles() {
    Map<Integer, List<BucketServerLocation66>> bucketToServerLocations = new HashMap<>();
    for (Integer bucketId : clientBucketProfilesMap.keySet()) {
      ArrayList<BucketServerLocation66> clientBucketProfiles = new ArrayList<>();
      for (BucketProfile profile : clientBucketProfilesMap.get(bucketId)) {
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
          List<BucketServerLocation66> clientBucketProfiles =
              bucketToServerLocations.computeIfAbsent(bucketId, k -> new ArrayList<>());
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

  @VisibleForTesting
  public ConcurrentHashMap<Integer, Set<ServerBucketProfile>> getAllClientBucketProfilesTest() {
    ConcurrentHashMap<Integer, Set<ServerBucketProfile>> map = new ConcurrentHashMap<>();
    Map<Integer, List<BucketServerLocation66>> testMap =
        new HashMap<>(getAllClientBucketProfiles());
    for (Integer bucketId : testMap.keySet()) {
      Set<ServerBucketProfile> parr = new HashSet<>(clientBucketProfilesMap.get(bucketId));
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
    return clientBucketProfilesMap.get(bucketId);
  }

  public void setClientBucketProfiles(Integer bucketId, Set<ServerBucketProfile> profiles) {
    clientBucketProfilesMap.put(bucketId, Collections.unmodifiableSet(profiles));
  }

  /**
   * Close the bucket advisors, releasing any locks for primary buckets
   */
  public void closeBucketAdvisors() {
    if (buckets != null) {
      for (ProxyBucketRegion pbr : buckets) {
        pbr.close();
      }
    }
  }

  /**
   * Close the adviser and all bucket advisors.
   */
  @Override
  public void close() {
    super.close();
    if (buckets != null) {
      for (ProxyBucketRegion bucket : buckets) {
        bucket.close();
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
    synchronized (preInitQueueMonitor) {
      if (preInitQueue != null) {
        // Queue profile during pre-initialization
        QueuedBucketProfile qbf = new QueuedBucketProfile((InternalDistributedMember) memberId,
            crashed, regionDestroyed, fromMembershipListener);
        preInitQueue.add(qbf);
        removeBuckets = false;
      }
    } // synchronized
    if (removeBuckets && buckets != null) {
      for (ProxyBucketRegion pbr : buckets) {
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

    boolean removedId;
    removedId = super.removeId(memberId, crashed, regionDestroyed, fromMembershipListener);
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "RegionAdvisor#removeId: removing member from region {}: {}; removed = {}; crashed = {}",
          getPartitionedRegion().getName(), memberId, removedId, crashed);
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
  public void removeIdAndBuckets(InternalDistributedMember memberId, int prSerial, int[] serials,
      boolean regionDestroyed) {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "RegionAdvisor#removeIdAndBuckets: removing member from region {}: {}; buckets = ({}) serials",
          getPartitionedRegion().getName(), memberId,
          (serials == null ? "null" : serials.length));
    }

    synchronized (preInitQueueMonitor) {
      if (preInitQueue != null) {
        // Queue profile during pre-initialization
        QueuedBucketProfile qbf = new QueuedBucketProfile(memberId, serials, regionDestroyed);
        preInitQueue.add(qbf);
        return;
      }
    }

    // OK, apply the update NOW
    if (buckets != null) {
      Objects.requireNonNull(serials);
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "RegionAdvisor#removeIdAndBuckets: removing buckets for member{};{}", memberId, this);
      }
      for (int i = 0; i < buckets.length; i++) {
        int s = serials[i];
        if (s != ILLEGAL_SERIAL) {
          if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
            logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                "RegionAdvisor#removeIdAndBuckets: removing bucket #{} serial {}", i, s);
          }
          buckets[i].getBucketAdvisor().removeIdWithSerial(memberId, s, regionDestroyed);
        }
      }

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
      if (sick && !buckets[i].getBucketOwners().contains(member)) {
        continue;
      }
      buckets[i].setBucketSick(member, sick);
      if (logger.isDebugEnabled()) {
        logger.debug("Marked bucket ({}) {}", getPartitionedRegion().bucketStringForLogs(i),
            (buckets[i].isBucketSick() ? "sick" : "healthy"));
      }
    }
  }

  public void updateBucketStatus(int bucketId, DistributedMember member, boolean profileRemoved) {
    if (profileRemoved) {
      buckets[bucketId].setBucketSick(member, false);

    } else {
      ResourceAdvisor advisor = getPartitionedRegion().getCache().getResourceAdvisor();
      boolean sick = advisor.adviseCriticalMembers().contains(member);
      if (logger.isDebugEnabled()) {
        logger.debug("updateBucketStatus:({}):member:{}:sick:{}",
            getPartitionedRegion().bucketStringForLogs(bucketId), member, sick);
      }
      buckets[bucketId].setBucketSick(member, sick);
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
    if (buckets[bucketId].isBucketSick()) {
      Set<DistributedMember> sm = buckets[bucketId].getSickMembers();
      if (sm.isEmpty()) {
        // check again as this list is obtained under synchronization
        // fixes bug 50845
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("For bucket {} sick members are {}.",
            getPartitionedRegion().bucketStringForLogs(bucketId), sm);
      }
      throw new LowMemoryException(String.format(
          "PartitionedRegion: %s cannot process operation on key %s because members %s are running low on memory",
          getPartitionedRegion().getFullPath(), key, sm), sm);
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
      isPartitioned = true;
    }

    @Override
    protected int getIntInfo() {
      int s = super.getIntInfo();
      if (requiresNotification)
        s |= REQUIRES_NOTIFICATION_MASK;
      return s;
    }

    @Override
    protected void setIntInfo(int s) {
      super.setIntInfo(s);
      requiresNotification = (s & REQUIRES_NOTIFICATION_MASK) != 0;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      localMaxMemory = in.readInt();
      isDataStore = localMaxMemory > 0;
      fixedPAttrs = DataSerializer.readObject(in);
      shutDownAllStatus = in.readInt();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(localMaxMemory);
      DataSerializer.writeObject(fixedPAttrs, out);
      out.writeInt(shutDownAllStatus);
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("RegionAdvisor.PartitionProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; isDataStore=").append(isDataStore).append("; requiresNotification=")
          .append(requiresNotification).append("; localMaxMemory=").append(localMaxMemory)
          .append("; numBuckets=").append(numBuckets);
      if (fixedPAttrs != null) {
        sb.append("; FixedPartitionAttributes=").append(fixedPAttrs);
      }
      sb.append("; filterProfile=").append(filterProfile);
      sb.append("; shutDownAllStatus=").append(shutDownAllStatus);
    }

    @Override
    public int getDSFID() {
      return PARTITION_PROFILE;
    }


  } // end class PartitionProfile

  public int getNumDataStores() {
    final int numProfs = getNumProfiles();
    if (lastActiveProfiles != numProfs) {
      numDataStores = adviseDataStore().size();
      lastActiveProfiles = numProfs;
    }
    return numDataStores;
  }

  public Set<InternalDistributedMember> adviseDataStore() {
    return adviseDataStore(false);
  }

  /**
   * Returns the set of data stores that have finished initialization.
   */
  public Set<InternalDistributedMember> adviseInitializedDataStore() {
    return adviseFilter(profile -> {
      // probably not needed as all profiles for a partitioned region are Partition profiles
      if (profile instanceof PartitionProfile) {
        PartitionProfile p = (PartitionProfile) profile;
        return p.isDataStore && (!p.dataPolicy.withPersistence() || p.regionInitialized);
      }
      return false;
    });
  }

  /**
   * Returns the set of members that are not arrived at specified shutDownAll status
   */
  private Set<InternalDistributedMember> adviseNotAtShutDownAllStatus(final int status) {
    return adviseFilter(profile -> {
      // probably not needed as all profiles for a partitioned region are Partition profiles
      if (profile instanceof PartitionProfile) {
        PartitionProfile p = (PartitionProfile) profile;
        return p.isDataStore && p.shutDownAllStatus < status;
      }
      return false;
    });
  }

  public void waitForProfileStatus(int status) {
    ProfileShutdownListener listener = new ProfileShutdownListener();
    addProfileChangeListener(listener);
    try {
      int memberNum;
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
    Set<InternalDistributedMember> s = adviseFilter(profile -> {
      // probably not needed as all profiles for a partitioned region are Partition profiles
      if (profile instanceof PartitionProfile) {
        PartitionProfile p = (PartitionProfile) profile;
        return p.isDataStore;
      }
      return false;
    });

    if (realHashSet) {
      if (s == Collections.EMPTY_SET) {
        s = new HashSet<>();
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
    Set<InternalDistributedMember> s = adviseFilter(profile -> {
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
    });

    if (s == Collections.EMPTY_SET) {
      s = new HashSet<>();
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
    final List<InternalDistributedMember> fixedPartitionDataStore = new ArrayList<>(1);
    fetchProfiles(profile -> {
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
    final List<FixedPartitionAttributesImpl> allFPAs = new ArrayList<>();
    fetchProfiles(profile -> {
      if (profile instanceof PartitionProfile) {
        final PartitionProfile pp = (PartitionProfile) profile;
        if (pp.fixedPAttrs != null) {
          allFPAs.addAll(pp.fixedPAttrs);
          return true;
        }
      }
      return false;
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
    final List<FixedPartitionAttributesImpl> sameFPAs = new ArrayList<>();

    fetchProfiles(profile -> {
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
    final List<FixedPartitionAttributesImpl> remotePrimaryFPAs = new ArrayList<>();

    fetchProfiles(profile -> {
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
    });
    return remotePrimaryFPAs;
  }

  public Set<InternalDistributedMember> adviseAllPRNodes() {
    return adviseFilter(profile -> {
      CacheProfile prof = (CacheProfile) profile;
      return prof.isPartitioned;
    });
  }

  Set adviseAllServersWithInterest() {
    return adviseFilter(profile -> {
      CacheProfile prof = (CacheProfile) profile;
      return prof.hasCacheServer && prof.filterProfile != null
          && prof.filterProfile.hasInterest();
    });
  }

  @Immutable
  private static final Filter prServerWithInterestFilter = profile -> {
    CacheProfile prof = (CacheProfile) profile;
    return prof.isPartitioned && prof.hasCacheServer && prof.filterProfile != null
        && prof.filterProfile.hasInterest();
  };

  public boolean hasPRServerWithInterest() {
    return satisfiesFilter(prServerWithInterestFilter);
  }

  /**
   * return the set of all members who must receive operation notifications
   *
   * @since GemFire 5.1
   */
  public Set<InternalDistributedMember> adviseRequiresNotification() {
    return adviseFilter(profile -> {
      if (profile instanceof PartitionProfile) {
        PartitionProfile prof = (PartitionProfile) profile;
        if (prof.isPartitioned) {
          if (prof.hasCacheListener) {
            InterestPolicy pol = prof.subscriptionAttributes.getInterestPolicy();
            if (pol == InterestPolicy.ALL) {
              return true;
            }
          }
          return prof.requiresNotification;
        }
      }
      return false;
    });
  }

  @Override
  public synchronized boolean putProfile(Profile p) {
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
    if (buckets == null) {
      return false;
    }
    return buckets[bucketId].isPrimary();
  }

  /**
   * Returns true if the bucket is currently being hosted locally. Note that as soon as this call
   * returns, this datastore may begin to host the bucket, thus two calls in a row may be different.
   *
   * @param bucketId the index of the bucket to check
   * @return true if the bucket is currently being hosted locally
   */
  public boolean isBucketLocal(int bucketId) {
    if (buckets == null) {
      return false;
    }
    return buckets[bucketId].getHostedBucketRegion() != null;
  }

  public boolean areBucketsInitialized() {
    return buckets != null;
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
    ProxyBucketRegion pbr = buckets[bucketId];
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
    ProxyBucketRegion pbr = buckets[bucketId];
    Bucket ret = pbr.getHostedBucketRegion();
    if (ret != null) {
      return ret.getBucketAdvisor();
    } else {
      return pbr.getBucketAdvisor();
    }
  }

  public Map<Integer, BucketAdvisor> getAllBucketAdvisors() {
    Map<Integer, BucketAdvisor> map = new HashMap<>();
    for (ProxyBucketRegion pbr : buckets) {
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
    if (buckets == null) {
      return new int[0];
    }
    int[] result = new int[buckets.length];

    for (int i = 0; i < result.length; i++) {
      ProxyBucketRegion pbr = buckets[i];
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
    synchronized (preInitQueueMonitor) {
      boolean interrupted = false;
      try {
        while (preInitQueue != null) {
          try {
            preInitQueueMonitor.wait(); // spurious wakeup ok
          } catch (InterruptedException e) {
            interrupted = true;
            getAdvisee().getCancelCriterion().checkCancelInProgress(e);
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
    Bucket b = buckets[bucketId];
    return b.getBucketAdvisor().getPrimary();
  }

  /**
   * Return the node favored for reading for the given bucket
   *
   * @param bucketId the bucket we want to read
   * @return the member, possibly null if no member is available
   */
  public InternalDistributedMember getPreferredNode(int bucketId) {
    Bucket b = buckets[bucketId];
    return b.getBucketAdvisor().getPreferredNode();
  }

  public boolean isStorageAssignedForBucket(int bucketId) {
    return buckets[bucketId].getBucketRedundancy() >= 0;
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
      return buckets[bucketId].getBucketAdvisor().waitForRedundancy(minRedundancy);
    }
  }

  /**
   * Get the redundancy of the this bucket, taking into account the local bucket, if any.
   *
   * @return number of redundant copies for a given bucket, or -1 if there are no instances of the
   *         bucket.
   */
  public int getBucketRedundancy(int bucketId) {
    return buckets[bucketId].getBucketRedundancy();
  }

  /**
   * Return the set of all members who currently own the bucket, including the local owner, if
   * applicable
   *
   * @return a set of {@link InternalDistributedMember}s that own the bucket
   */
  public Set<InternalDistributedMember> getBucketOwners(int bucketId) {
    return buckets[bucketId].getBucketOwners();
  }

  /**
   * Return the set of buckets which have storage assigned
   *
   * @return set of Integer bucketIds
   */
  public Set<Integer> getBucketSet() {
    return new BucketSet();
  }


  public ProxyBucketRegion[] getProxyBucketArray() {
    return buckets;
  }

  private class BucketSet extends AbstractSet<Integer> {
    final ProxyBucketRegion[] pbrs;

    BucketSet() {
      pbrs = buckets;
    }

    /*
     * Note: The consistency between size(), hasNext() and next() is weak, meaning that the state of
     * the backing Set may change causing more or less elements to be available after calling size()
     */
    @Override
    public int size() {
      return pbrs.length;
    }

    @Override
    public Iterator<Integer> iterator() {
      return new BucketSetIterator();
    }

    class BucketSetIterator implements Iterator<Integer> {
      private int currentItem = -1;


      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      /*
       * Note: The consistency guarantee between hasNext() and next() is weak. It's possible
       * hasNext() will return true and a following call to next() may throw NoSuchElementException
       * (due to loss of bucket storage). Its also equally possible for hasNext() to return false
       * and a subsequent call to next() will return a valid bucketid.
       */
      @Override
      public boolean hasNext() {
        if (getPartitionedRegion().isFixedPartitionedRegion()) {
          if (currentItem + 1 < pbrs.length) {
            int possibleBucketId = currentItem;
            boolean bucketExists = false;

            List<FixedPartitionAttributesImpl> fpaList = adviseAllFixedPartitionAttributes();
            List<FixedPartitionAttributesImpl> localFpas =
                getPartitionedRegion().getFixedPartitionAttributesImpl();
            if (localFpas != null) {
              fpaList.addAll(localFpas);
            }
            while (++possibleBucketId < pbrs.length && !bucketExists) {
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
          return currentItem + 1 < pbrs.length;
        }
      }

      @Override
      public Integer next() {
        if (++currentItem < pbrs.length) {
          if (isStorageAssignedForBucket(currentItem)) {
            return currentItem;
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
                  if (fpa.hasBucket(currentItem)) {
                    bucketExists = true;
                    break;
                  }
                }
                if (!bucketExists) {
                  currentItem++;
                }
              } while (currentItem < pbrs.length && !bucketExists);

              if (bucketExists) {
                getPartitionedRegion().createBucket(currentItem, 0, null);
                return currentItem;
              }
            } else {
              getPartitionedRegion().createBucket(currentItem, 0, null);
              return currentItem;
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
    final HashMap<InternalDistributedMember, Integer> memberToPrimaryCount = new HashMap<>();
    for (ProxyBucketRegion pbr : buckets) {
      // quick dirty check
      InternalDistributedMember p = pbr.getBucketAdvisor().basicGetPrimaryMember();
      if (p != null) {
        memberToPrimaryCount.merge(p, 1, Integer::sum);
      }
    }

    final ArrayList<DataStoreBuckets> ds = new ArrayList<>(memberFilter.size());
    adviseFilter(profile -> {
      if (profile instanceof PartitionProfile) {
        PartitionProfile p = (PartitionProfile) profile;
        if (memberFilter.contains(p.getDistributedMember())) {
          Integer priCount = memberToPrimaryCount.get(p.getDistributedMember());
          int primaryCount = 0;
          if (priCount != null) {
            primaryCount = priCount;
          }
          ds.add(new DataStoreBuckets(p.getDistributedMember(), p.numBuckets, primaryCount,
              p.localMaxMemory));
        }
      }
      return false;
    });


    return ds;
  }

  public void incrementBucketCount(Profile p) {
    PartitionProfile pp = (PartitionProfile) getProfile(p.getDistributedMember());
    if (pp != null) {
      pp.numBuckets++;
    }
  }

  public void decrementsBucketCount(Profile p) {
    PartitionProfile pp = (PartitionProfile) getProfile(p.getDistributedMember());
    if (pp != null) {
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
      logger.debug("[dumpProfiles] dumping {}", toStringWithProfiles());
    }

    // 1st dump all profiles for this RegionAdvisor
    super.dumpProfiles(infoMsg);

    // 2nd dump all profiles for each BucketAdvisor
    ProxyBucketRegion[] pbrs = buckets;
    if (pbrs == null) {
      return;
    }
    for (ProxyBucketRegion pbr : pbrs) {
      pbr.getBucketAdvisor().dumpProfiles(infoMsg);
      BucketPersistenceAdvisor persistentAdvisor = pbr.getPersistenceAdvisor();
      if (persistentAdvisor != null) {
        persistentAdvisor.dump(infoMsg);
      }
    }
  }

  public void notPrimary(int bucketId, InternalDistributedMember wasPrimary) {
    ProxyBucketRegion b = buckets[bucketId];
    b.getBucketAdvisor().notPrimary(wasPrimary);
  }

  /**
   * Find the set of members which own primary buckets, including the local member
   *
   * @return set of InternalDistributedMember ids
   */
  public Set advisePrimaryOwners() {
    ProxyBucketRegion[] bucs = buckets;
    HashSet<InternalDistributedMember> hs = new HashSet<>();
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
    final ProxyBucketRegion[] bucs = buckets;
    Objects.requireNonNull(bucs);
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
  void putBucketProfile(int bucketId, BucketProfile profile) {
    synchronized (preInitQueueMonitor) {
      if (preInitQueue != null) {
        // Queue profile during pre-initialization
        QueuedBucketProfile qbf = new QueuedBucketProfile(bucketId, profile);
        preInitQueue.add(qbf);
        return;
      }
    }

    // Directly process profile post-initialization
    getBucket(bucketId).getBucketAdvisor().putProfile(profile);
  }

  static class QueuedBucketProfile {
    protected final int bucketId;
    final BucketProfile bucketProfile;

    /** true means that this member has departed the view */
    protected final boolean memberDeparted;

    /** true means that this profile needs to be removed */
    final boolean isRemoval;

    /** true means that the peer crashed */
    protected final boolean crashed;

    /**
     * true means that this QueuedBucketProfile was created because of MembershipListener invocation
     */
    final boolean fromMembershipListener;

    protected final boolean destroyed;

    protected final InternalDistributedMember memberId;
    final int[] serials;

    /**
     * Queue up an addition
     *
     * @param bId the bucket being added
     * @param p the profile to add
     */
    QueuedBucketProfile(int bId, BucketProfile p) {
      bucketId = bId;
      bucketProfile = p;
      isRemoval = false;
      crashed = false;
      memberDeparted = false;
      memberId = null;
      serials = null;
      destroyed = false;
      fromMembershipListener = false;
    }

    /**
     * Queue up a removal due to member leaving the view
     *
     * @param mbr the member being removed
     */
    QueuedBucketProfile(InternalDistributedMember mbr, boolean crashed, boolean destroyed,
        boolean fromMembershipListener) {
      bucketId = 0;
      bucketProfile = null;
      isRemoval = true;
      this.crashed = crashed;
      memberDeparted = true;
      memberId = mbr;
      serials = null;
      this.destroyed = destroyed;
      this.fromMembershipListener = fromMembershipListener;
    }

    /**
     * Queue up a removal due to region destroy
     *
     * @param mbr the member being removed
     * @param serials the serials it had
     */
    QueuedBucketProfile(InternalDistributedMember mbr, int[] serials, boolean destroyed) {
      bucketId = 0;
      bucketProfile = null;
      isRemoval = true;
      crashed = false;
      memberDeparted = false;
      memberId = mbr;
      this.serials = serials;
      this.destroyed = destroyed;
      fromMembershipListener = false;
    }
  }

  public Set<InternalDistributedMember> adviseBucketProfileExchange() {
    return adviseDataStore();
  }

  public long adviseTotalMemoryAllocation() {
    final AtomicLong total = new AtomicLong();
    adviseFilter(profile -> {
      // probably not needed as all profiles for a partitioned region are Partition profiles
      if (profile instanceof PartitionProfile) {
        PartitionProfile p = (PartitionProfile) profile;
        total.addAndGet(p.localMaxMemory);
      }
      return false;
    });
    return total.get();
  }

  /**
   * Returns the total number of buckets created anywhere in the distributed system for this
   * partitioned region.
   *
   * @return the total number of buckets created anywhere for this PR
   */
  public int getCreatedBucketsCount() {
    final ProxyBucketRegion[] bucs = buckets;
    if (bucs == null) {
      return 0;
    }
    int createdBucketsCount = 0;
    for (ProxyBucketRegion buc : bucs) {
      if (buc.getBucketOwnersCount() > 0) {
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
    final ProxyBucketRegion[] bucs = buckets;
    if (bucs == null) {
      return null;
    }
    ArrayList<BucketProfileAndId> result = new ArrayList<>(bucs.length);
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
  public void putBucketRegionProfiles(ArrayList<BucketProfileAndId> l) {
    for (BucketProfileAndId bp : l) {
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
      ((PartitionedRegion) getAdvisee()).removeCriticalMember(profile.peerMemberId);
    }

    if (buckets != null) {
      for (ProxyBucketRegion bucket : buckets) {
        bucket.getBucketAdvisor().checkForLostPrimaryElector(profile);
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
      return id;
    }

    BucketProfile getBucketProfile() {
      return bp;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = in.readInt();
      isServerBucketProfile = in.readBoolean();
      if (isServerBucketProfile)
        bp = new ServerBucketProfile();
      else
        bp = new BucketProfile();

      InternalDataSerializer.invokeFromData(bp, in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(id);
      out.writeBoolean(isServerBucketProfile);
      InternalDataSerializer.invokeToData(bp, out);
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

    void waitForChange() {
      Region pr = getPartitionedRegion();

      synchronized (this) {
        while (!profileChanged && pr != null && !pr.isDestroyed()) {
          // the advisee might have been destroyed due to initialization failure
          try {
            wait(1000);
          } catch (InterruptedException ignored) {
          }
        }
        profileChanged = false;
      }
    }

    @Override
    public void profileCreated(Profile profile) {
      profileUpdated(profile);
    }

    @Override
    public void profileRemoved(Profile profile, boolean regionDestroyed) {
      // if a profile is gone, notify
      synchronized (this) {
        profileChanged = true;
        notifyAll();
      }
    }

    @Override
    public void profileUpdated(Profile profile) {
      // when updated, notify the loop in GFC to check the list again
      synchronized (this) {
        profileChanged = true;
        notifyAll();
      }
    }
  }
}
