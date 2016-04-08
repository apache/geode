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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.BucketPersistenceAdvisor;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.ProxyBucketRegion;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentStateListener;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.process.StartupStatus;
import com.gemstone.gemfire.internal.util.TransformUtils;

/**
 * Consolidates logging during the recovery of ProxyRegionBuckets that are not hosted by this member.  This logger
 * is meant to run in its own thread and utilizes the PRHARedundancyProvider's count down latch in order to determine
 * when it is finished.
 * 
 */
public class RedundancyLogger extends RecoveryRunnable implements PersistentStateListener {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * True when one or more buckets have reported a change in status.
   */
  private volatile boolean membershipChanged = true; 
  
  /**
   * Sleep period between posting log entries.
   */
  private static final int SLEEP_PERIOD = 15000;
  
  /**
   * Used to determine when all proxy buckets have been recovered.
   */
  private final CountDownLatch allBucketsRecoveredFromDisk;

  private final List<RegionStatus> regions;
  

  /**
   * Creates a new RedundancyLogger.
   * @param prhaRedundancyProvider
   */
  public RedundancyLogger(PRHARedundancyProvider prhaRedundancyProvider) {
    super(prhaRedundancyProvider);
    PartitionedRegion baseRegion = ColocationHelper.getLeaderRegion(redundancyProvider.prRegion);
    List<PartitionedRegion> colocatedRegions = ColocationHelper.getColocatedChildRegions(baseRegion);
    List<RegionStatus> allRegions = new ArrayList<RegionStatus>(colocatedRegions.size() + 1);
    if(baseRegion.getDataPolicy().withPersistence()) {
      allRegions.add(new RegionStatus(baseRegion));
    }
    for(PartitionedRegion region : colocatedRegions) {
      if(region.getDataPolicy().withPersistence()) {
        allRegions.add(new RegionStatus(region));
      }
    }
    
    this.regions = Collections.unmodifiableList(allRegions);
    
    
    this.allBucketsRecoveredFromDisk = redundancyProvider.getAllBucketsRecoveredFromDiskLatch();
    this.membershipChanged = true;
    addListeners();
  }

  /**
   * Called when a member comes online for a bucket.
   */
  @Override
  public void memberOnline(InternalDistributedMember member,
      PersistentMemberID persistentID) {
    this.membershipChanged = true;
  }

  /**
   * Called when a member goes offline for a bucket.
   */
  @Override
  public void memberOffline(InternalDistributedMember member,
      PersistentMemberID persistentID) {
    this.membershipChanged = true;
  }

  /**
   * Called when a member is removed for a bucket.
   */
  @Override
  public void memberRemoved(PersistentMemberID persistentID, boolean revoked) {
    this.membershipChanged = true;
  }

  

  /**
   * Add this RedundancyLogger as a persistence listener to all the region's bucket advisors.
   */
  private void addListeners() {
    for(RegionStatus region : regions) {
      region.addListeners();
    }
  }
  
  /**
   * Removes this RedundancyLogger as a persistence listener from all the region's bucket advisors.
   */
  private void removeListeners() {
    for(RegionStatus region : regions) {
      region.removeListeners();
    }
  }
  
  

  /**
   * Writes a consolidated log entry every SLEEP_PERIOD that summarizes which buckets are still waiting on persistent members
   * for the region.
   */
  @Override
  public void run2() {
    try{      
      boolean warningLogged = false;
      while(this.allBucketsRecoveredFromDisk.getCount() > 0) {
        int sleepMillis = SLEEP_PERIOD;
        // reduce the first log time from 15secs so that higher layers can
        // report sooner to user
        if (!warningLogged) {
          sleepMillis = SLEEP_PERIOD / 2;
        }
        Thread.sleep(sleepMillis);          

        if(this.membershipChanged) {
          this.membershipChanged = false;
          for(RegionStatus region : regions) {
            region.logWaitingForMembers();
          }
          warningLogged = true;
        }
      }
      
    } catch (InterruptedException e) {
      // Log and bail
      logger.error(e.getMessage(), e);
    } finally {        
      /*
       * Our job is done.  Stop listening to the bucket advisors.
       */
      removeListeners();
      /*
       * Make sure the recovery completion message was printed to the log.
       */
      for(RegionStatus region : regions) {
        if(!region.loggedDoneMessage) {
          region.logDoneMessage();
        }
      }
    }
  }
  

  /**
   * Keeps track of logging a message for a single partitioned region
   * and logging a separate message when the waiting is done for the same region
   *
   */
  private class RegionStatus {
    /**
     * The persistent identifier of the member running this RedundancyLogger.
     */
    private final PersistentMemberID thisMember;
    
    /**
     * The region that the proxy buckets belong to.
     */
    private final String region;
    
    /**
     * An array of ProxyBucketRegions that comprise this partitioned region.
     */
    private final ProxyBucketRegion[] bucketRegions;
    
    /**
     * Indicates that a completion message has been logged.
     */
    private volatile boolean loggedDoneMessage = true;
    
    public RegionStatus(PartitionedRegion region) {
      this.thisMember = createPersistentMemberID(region);
      this.region = region.getFullPath();
      this.bucketRegions = region.getRegionAdvisor().getProxyBucketArray();
    }

    public void removeListeners() {
      for(ProxyBucketRegion proxyBucket : this.bucketRegions) {
        proxyBucket.getPersistenceAdvisor().removeListener(RedundancyLogger.this);
      }
    }

    public void addListeners() {
      for(ProxyBucketRegion proxyBucket : this.bucketRegions) {
        proxyBucket.getPersistenceAdvisor().addListener(RedundancyLogger.this);
      }
    }

    /**
     * Creates a temporary (and somewhat fake) PersistentMemberID for this member if 
     * there is no DiskStore available for our region (which can happen in some
     * colocated scenarios).
     */
    private PersistentMemberID createPersistentMemberID(PartitionedRegion region) {
      DiskStoreImpl diskStore = null;
      
      /*
       * A non-persistent colocated region will not have a disk store so check the leader 
       * region if this region does not have one.
       */
      if(region.getAttributes().getDataPolicy().withPersistence()) {
        diskStore = region.getDiskStore();
      } else if(ColocationHelper.getLeaderRegion(region).getAttributes().getDataPolicy().withPersistence()) {
        diskStore = ColocationHelper.getLeaderRegion(region).getDiskStore();
      }
      
      /*
       * We have a DiskStore?  Great!  Simply have it 
       * generate the id.
       */
      if(null != diskStore) {
        return diskStore.generatePersistentID(null);
      }

      /*
       * Bummer.  No DiskStore.  Put together a fake one (for logging only).
       */
      {
        String name = "No name for this member";
        String diskDir = System.getProperty("user.dir");      
        InetAddress localHost = null;

        try {
          localHost = SocketCreator.getLocalHost();
        } catch (UnknownHostException e) {
          logger.error("Could not determine my own host", e);
        }

        return (new PersistentMemberID(null, localHost, diskDir, name, redundancyProvider.prRegion.getCache().cacheTimeMillis(), (short) 0));    
      }
    }
    
    /**
     * Returns a unique Set of persistent members that all the ProxyBucketRegions are waiting for.
     * @param offlineOnly true if only the members which are not currently try running should be returned,
     * false to return all members that this member is waiting for, including members which are running
     * but not fully initialized.
     */
    private Map<PersistentMemberID, Set<Integer>> getMembersToWaitFor(boolean offlineOnly) {
      Map<PersistentMemberID, Set<Integer>> waitingForMembers = new HashMap<PersistentMemberID, Set<Integer>>();
      
      
      for(ProxyBucketRegion proxyBucket : this.bucketRegions) {
        Integer bucketId = proxyBucket.getBucketId();
        
        //Get the set of missing members from the persistence advisor
        Set<PersistentMemberID> missingMembers;
        BucketPersistenceAdvisor persistenceAdvisor = proxyBucket.getPersistenceAdvisor();
        if(offlineOnly) {
          missingMembers = persistenceAdvisor.getMissingMembers();
        } else {
          missingMembers = persistenceAdvisor.getAllMembersToWaitFor();
        }
        
        if(missingMembers != null) {
          for(PersistentMemberID missingMember : missingMembers) {
            Set<Integer> buckets = waitingForMembers.get(missingMember);
            if(buckets == null) {
              buckets = new TreeSet<Integer>();
              waitingForMembers.put(missingMember, buckets);
            }
            buckets.add(bucketId);
          }
        }
      }
      
      return waitingForMembers;
    }

    /**
     * Prints a recovery completion message to the log.
     */
    private void logDoneMessage() {
      this.loggedDoneMessage = true;          
      StartupStatus.startup(LocalizedStrings.CreatePersistentRegionProcessor_DONE_WAITING_FOR_BUCKET_MEMBERS, new Object[] {this.region, TransformUtils.persistentMemberIdToLogEntryTransformer.transform(this.thisMember)});
    }
    
    /**
     * Logs a consolidated log entry for all ProxyBucketRegions waiting for persistent members.
     */
    private void logWaitingForMembers() {
      Map<PersistentMemberID, Set<Integer>> offlineMembers = getMembersToWaitFor(true);
      Map<PersistentMemberID, Set<Integer>> allMembersToWaitFor= getMembersToWaitFor(false);
      
      boolean thereAreBucketsToBeRecovered = (RedundancyLogger.this.allBucketsRecoveredFromDisk.getCount() > 0);
      
      /*
       * Log any offline members the region is waiting for.
       */
      if(thereAreBucketsToBeRecovered && !offlineMembers.isEmpty()) {
        Set<String> membersToWaitForLogEntries = new HashSet<String>();
        
        TransformUtils.transform(offlineMembers.entrySet(), membersToWaitForLogEntries, TransformUtils.persistentMemberEntryToLogEntryTransformer);
        
        Set<Integer> missingBuckets = getAllWaitingBuckets(offlineMembers);
        
        StartupStatus
        	.startup(LocalizedStrings.CreatePersistentRegionProcessor_WAITING_FOR_OFFLINE_BUCKET_MEMBERS, 
        		new Object[] {this.region, missingBuckets, TransformUtils.persistentMemberIdToLogEntryTransformer.transform(this.thisMember),
                membersToWaitForLogEntries});

        this.loggedDoneMessage = false;
      } 
      /*
       * No offline? Then log any online members the region is waiting for.
       */
      else if(thereAreBucketsToBeRecovered && !allMembersToWaitFor.isEmpty()){
        Set<String> membersToWaitForLogEntries = new HashSet<String>();
        
        Set<Integer> missingBuckets = getAllWaitingBuckets(allMembersToWaitFor);
        TransformUtils.transform(allMembersToWaitFor.entrySet(), membersToWaitForLogEntries, TransformUtils.persistentMemberEntryToLogEntryTransformer);

        StartupStatus
        	.startup(LocalizedStrings.CreatePersistentRegionProcessor_WAITING_FOR_ONLINE_BUCKET_MEMBERS, 
        		new Object[] {this.region, missingBuckets, TransformUtils.persistentMemberIdToLogEntryTransformer.transform(this.thisMember),
        		membersToWaitForLogEntries});


        this.loggedDoneMessage = false;
      }
      /*
       * No online? Then log that we are done. 
       */
      else if(!this.loggedDoneMessage) {
        logDoneMessage();
      }
    }
    
    /**
     * Get a consolodated set of all buckets that are waiting.
     */
    private Set<Integer> getAllWaitingBuckets(
        Map<PersistentMemberID, Set<Integer>> offlineMembers) {
      Set<Integer> allWaitingBuckets = new TreeSet<Integer>();
      for(Set<Integer> missingPerMember : offlineMembers.values()) {
        allWaitingBuckets.addAll(missingPerMember);
      }
      return allWaitingBuckets;
    }
  }
}

