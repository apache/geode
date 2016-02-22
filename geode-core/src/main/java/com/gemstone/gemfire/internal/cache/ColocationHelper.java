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

package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.persistence.PRPersistentConfig;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * An utility class to retrieve colocated regions in a colocation hierarchy in
 * various scenarios
 * 
 * 
 * @since 6.0
 */
public class ColocationHelper {

  /** Logging mechanism for debugging */
  private static final Logger logger = LogService.getLogger();
   /**
    * An utility method to retrieve colocated region name of a given partitioned
    * region without waiting on initialize
    *
    * @param partitionedRegion
    * @return colocated PartitionedRegion
    * @since cheetah
    */
  public static PartitionedRegion getColocatedRegionName(
      final PartitionedRegion partitionedRegion) {
    Assert.assertTrue(partitionedRegion != null); // precondition1
    String colocatedWith = partitionedRegion.getPartitionAttributes().getColocatedWith();
    if (colocatedWith == null) {
      // the region is not colocated with any region
      return null;
    }
    PartitionedRegion colocatedPR = partitionedRegion.getColocatedWithRegion();
    if (colocatedPR != null && !colocatedPR.isLocallyDestroyed
        && !colocatedPR.isDestroyed()) {
      return colocatedPR;
    }
    Region prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion
        .getCache());
    PartitionRegionConfig prConf = (PartitionRegionConfig)prRoot
        .get(getRegionIdentifier(colocatedWith));
    int prID = -1; 
    try {
      if (prConf == null) {
        colocatedPR = getColocatedPR(partitionedRegion, colocatedWith);
      }
      else {
        prID = prConf.getPRId();
        colocatedPR = PartitionedRegion.getPRFromId(prID);
        if (colocatedPR == null && prID > 0) {
          // colocatedPR might have not called registerPartitionedRegion() yet, but since prID is valid,
          // we are able to get colocatedPR and do colocatedPR.waitOnBucketMetadataInitialization()
          colocatedPR = getColocatedPR(partitionedRegion, colocatedWith);
        }
      }
    }
    catch (PRLocallyDestroyedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("PRLocallyDestroyedException : Region with prId=" + prID
            + " is locally destroyed on this node", e);
      } 
    } 
    return colocatedPR;
  }
    private static PartitionedRegion getColocatedPR(
      final PartitionedRegion partitionedRegion, final String colocatedWith) {
    logger.info(LocalizedMessage.create(
        LocalizedStrings.HOPLOG_0_COLOCATE_WITH_REGION_1_NOT_INITIALIZED_YET,
        new Object[] { partitionedRegion.getFullPath(), colocatedWith }));
    PartitionedRegion colocatedPR = (PartitionedRegion) partitionedRegion
        .getCache().getPartitionedRegion(colocatedWith, false);
    assert colocatedPR != null;
    return colocatedPR;
  }
  /** Whether to ignore missing parallel queues on restart
   * if they are not attached to the region. See bug 50120. Mutable
   * for tests.
   */
  public static boolean IGNORE_UNRECOVERED_QUEUE = Boolean.getBoolean("gemfire.IGNORE_UNRECOVERED_QUEUE");

  /**
   * An utility method to retrieve colocated region of a given partitioned
   * region
   * 
   * @param partitionedRegion
   * @return colocated PartitionedRegion
   * @since 5.8Beta
   */
  public static PartitionedRegion getColocatedRegion(
      final PartitionedRegion partitionedRegion) {
    Assert.assertTrue(partitionedRegion != null); // precondition1
    String colocatedWith = partitionedRegion.getPartitionAttributes()
        .getColocatedWith();
    if (colocatedWith == null) {
      // the region is not colocated with any region
      return null;
    }
    Region prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion
        .getCache());
    PartitionRegionConfig prConf = (PartitionRegionConfig)prRoot
        .get(getRegionIdentifier(colocatedWith));
    int prID = prConf.getPRId();
    PartitionedRegion colocatedPR = null;
    try {
      colocatedPR = PartitionedRegion.getPRFromId(prID);
      colocatedPR.waitOnBucketMetadataInitialization();
    }
    catch (PRLocallyDestroyedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("PRLocallyDestroyedException : Region with prId={} is locally destroyed on this node", prID, e);
      }
    }
    return colocatedPR;
  }
  
  /**
   * An utility to make sure that a member contains all of the partitioned
   * regions that are colocated with a given region on other members.
   * TODO rebalance - this is rather inefficient, and probably all this junk should
   * be in the advisor.
   */
  public static boolean checkMembersColocation(PartitionedRegion partitionedRegion, InternalDistributedMember member) {
    List<PartitionRegionConfig> colocatedRegions = new ArrayList<PartitionRegionConfig>();
    List<PartitionRegionConfig> tempcolocatedRegions = new ArrayList<PartitionRegionConfig>();
    Region prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion
        .getCache());
    PartitionRegionConfig regionConfig =(PartitionRegionConfig) prRoot.get(partitionedRegion.getRegionIdentifier());
    //The region was probably concurrently destroyed
    if(regionConfig == null) {
      return false;
    }
    tempcolocatedRegions.add(regionConfig);
    colocatedRegions.addAll(tempcolocatedRegions);
    PartitionRegionConfig prConf = null;
    do {
      PartitionRegionConfig tempToBeColocatedWith = tempcolocatedRegions
          .remove(0);
      for (Iterator itr = prRoot.keySet().iterator(); itr.hasNext();) {
        String prName = (String)itr.next();
        try {
          prConf = (PartitionRegionConfig)prRoot.get(prName);
        }
        catch (EntryDestroyedException ede) {
          continue;
        }
        if (prConf == null) {
          // darrel says: I'm seeing an NPE in this code after pr->rem
          // merge so I added this check and continue
          continue;
        }
        if (prConf.getColocatedWith() != null) {
          if (prConf.getColocatedWith().equals(
              tempToBeColocatedWith.getFullPath())
              || ("/" + prConf.getColocatedWith())
              .equals(tempToBeColocatedWith.getFullPath())) {
            colocatedRegions.add(prConf);
            tempcolocatedRegions.add(prConf);
          }
        }
      }
    } while (!tempcolocatedRegions.isEmpty());

    PartitionRegionConfig tempColocatedWith = regionConfig;
    prConf = null;
    while (true) {
      String colocatedWithRegionName = tempColocatedWith.getColocatedWith();
      if (colocatedWithRegionName == null)
        break;
      else {
        try {
          prConf = (PartitionRegionConfig)prRoot
              .get(getRegionIdentifier(colocatedWithRegionName));
        }
        catch (EntryDestroyedException ede) {
          throw ede;
        }
        if (prConf == null) {
          break;
        }
        colocatedRegions.add(tempColocatedWith);
        tempColocatedWith = prConf;
      }
    }
    
    //Now check to make sure that all of the colocated regions
    //Have this member.

    //We don't need a hostname because the equals method doesn't check it.
    for(PartitionRegionConfig config: colocatedRegions) {
      if(config.isColocationComplete() && !config.containsMember(member)) {
        return false;
      }
    }

    //Check to make sure all of the persisted regions that are colocated
    //with this region have been created.
    if(hasOfflineColocatedChildRegions(partitionedRegion)) {
      return false;
    }
    
    return true;
  }
  
  /**
   * Returns true if there are regions that are persisted on this member and
   * were previously colocated with the given region, but have not yet been created.
   * 
   * @param region The parent region
   * @return true if there are any child regions that are persisted on this
   * member, but have not yet been created.
   */
  private static boolean hasOfflineColocatedChildRegions(PartitionedRegion region) {
    
    boolean hasOfflineChildren;
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
      GemFireCacheImpl cache = region.getCache();
      Collection<DiskStoreImpl> stores = cache.listDiskStores();
      //Look through all of the disk stores for offline colocated child regions
      for(DiskStoreImpl diskStore: stores) {
        //Look at all of the partitioned regions.
        for(Map.Entry<String, PRPersistentConfig> entry : diskStore.getAllPRs().entrySet()) {

          PRPersistentConfig config = entry.getValue();
          String childName = entry.getKey();

          //Check to see if they're colocated with this region.
          if(region.getFullPath().equals(config.getColocatedWith())) {
            PartitionedRegion childRegion = (PartitionedRegion) cache.getRegion(childName);

            if(childRegion == null) {
              //If the child region is offline, return true
              //unless it is a parallel queue that the user has removed.
              if(!ignoreUnrecoveredQueue(region, childName)) {
                return true;
              }
            } else {
              //Otherwise, look for offline children of that region.
              if(hasOfflineColocatedChildRegions(childRegion)) {
                return true;
              }
            }
          }
        }
      }
    
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return false;
  }
  

  private static boolean ignoreUnrecoveredQueue(PartitionedRegion region, String childName) {
    //Hack for #50120 if the childRegion is an async queue, but we
    //no longer define the async queue, ignore it.
    if(!ParallelGatewaySenderQueue.isParallelQueue(childName)) {
      return false;
    }
    
    String senderId = ParallelGatewaySenderQueue.getSenderId(childName);
    if(!region.getAsyncEventQueueIds().contains(senderId) 
        && !region.getParallelGatewaySenderIds().contains(senderId)
        && IGNORE_UNRECOVERED_QUEUE) {
      return true;
    }
    
    // TODO Auto-generated method stub
    return false;
  }

  /** A utility to check to see if a region has been created on
   * all of the VMs that host the regions this region is colocated with.
   */
  public static boolean isColocationComplete(PartitionedRegion region) {
    Region prRoot = PartitionedRegionHelper.getPRRoot(region
        .getCache());
    PartitionRegionConfig config = (PartitionRegionConfig) prRoot.get(region.getRegionIdentifier());
    //Fix for  bug 40075. There is race between this call and the region being concurrently
    //destroyed.
    if(config == null) {
      Assert.assertTrue(region.isDestroyed() || region.isClosed, "Region is not destroyed, but there is no entry in the prRoot for region " + region);
      return false;
    }
    return config.isColocationComplete();
  }
  
  /**
   * An utility method to retrieve all partitioned regions(excluding self) in a
   * colocation chain<br>
   * <p>
   * For example, shipmentPR is colocated with orderPR and orderPR is colocated
   * with customerPR <br>
   * <br>
   * getAllColocationRegions(customerPR) --> List{orderPR, shipmentPR}<br>
   * getAllColocationRegions(orderPR) --> List{customerPR, shipmentPR}<br>
   * getAllColocationRegions(shipmentPR) --> List{customerPR, orderPR}<br>
   * 
   * @param partitionedRegion
   * @return List of all partitioned regions (excluding self) in a colocated
   *         chain
   * @since 5.8Beta
   */
  public static Map<String, PartitionedRegion> getAllColocationRegions(
      PartitionedRegion partitionedRegion) {
    Map<String, PartitionedRegion> colocatedRegions = new HashMap<String, PartitionedRegion>();
    List<PartitionedRegion> colocatedByRegion = partitionedRegion.getColocatedByList();
    if (colocatedByRegion.size() != 0) {
      List<PartitionedRegion> tempcolocatedRegions = new ArrayList<PartitionedRegion>();
      tempcolocatedRegions.addAll(colocatedByRegion);
      do {
        PartitionedRegion pRegion = tempcolocatedRegions.remove(0);
        pRegion.waitOnBucketMetadataInitialization();
        colocatedRegions.put(pRegion.getFullPath(), pRegion);
        tempcolocatedRegions.addAll(pRegion.getColocatedByList());
      } while (!tempcolocatedRegions.isEmpty());
    }
    PartitionedRegion tempColocatedWith = partitionedRegion;
    while (true) {
      PartitionedRegion colocatedWithRegion = tempColocatedWith
          .getColocatedWithRegion();
      if (colocatedWithRegion == null)
        break;
      else {
        colocatedRegions.put(colocatedWithRegion.getFullPath(),
            colocatedWithRegion);
        tempColocatedWith = colocatedWithRegion;
      }
    }
    return colocatedRegions;
  }

  /**
   * gets local data of colocated regions on a particular data store
   * 
   * @param partitionedRegion
   * @return map of region name to local colocated regions
   * @since 5.8Beta
   */
  public static Map<String, Region> getAllColocatedLocalDataSets(
      PartitionedRegion partitionedRegion, InternalRegionFunctionContext context) {
    Map<String, PartitionedRegion> colocatedRegions = getAllColocationRegions(partitionedRegion);
    Map<String, Region> colocatedLocalRegions = new HashMap<String, Region>();
    for (Iterator itr = colocatedRegions.entrySet().iterator(); itr.hasNext();) {
      Map.Entry me = (Entry)itr.next();
      final Region pr = (Region) me.getValue();
      colocatedLocalRegions.put((String)me.getKey(), context.getLocalDataSet(pr));
    }
    return colocatedLocalRegions;
  }
  
  public static Map<String, LocalDataSet> constructAndGetAllColocatedLocalDataSet(
      PartitionedRegion region, Set<Integer> bucketSet) {
    Map<String, LocalDataSet> colocatedLocalDataSets = new HashMap<String, LocalDataSet>();
    if (region.getColocatedWith() == null && (!region.isColocatedBy())) {
      colocatedLocalDataSets.put(region.getFullPath(), new LocalDataSet(region, bucketSet));
      return colocatedLocalDataSets;
    }
    Map<String, PartitionedRegion> colocatedRegions = ColocationHelper
        .getAllColocationRegions(region);
    for (Region colocatedRegion : colocatedRegions.values()) {
      colocatedLocalDataSets.put(colocatedRegion.getFullPath(),
          new LocalDataSet((PartitionedRegion)colocatedRegion, bucketSet));
    }
    colocatedLocalDataSets.put(region.getFullPath(), new LocalDataSet(region, bucketSet));
    return colocatedLocalDataSets;
  }
  
  public static Map<String, LocalDataSet> getColocatedLocalDataSetsForBuckets(PartitionedRegion region, Set<Integer> bucketSet) {
    if (region.getColocatedWith() == null && (!region.isColocatedBy())) {
      return Collections.emptyMap();
    }
    Map<String, LocalDataSet> ret = new HashMap<String, LocalDataSet>();
    Map<String, PartitionedRegion> colocatedRegions = ColocationHelper
        .getAllColocationRegions(region);
    for (Region colocatedRegion : colocatedRegions.values()) {
      ret.put(colocatedRegion.getFullPath(),
          new LocalDataSet((PartitionedRegion)colocatedRegion, bucketSet));
    }
    return ret;
  }

  /**
   * A utility method to retrieve all child partitioned regions that are
   * directly colocated to the specified partitioned region.<br>
   * <p>
   * For example, shipmentPR is colocated with orderPR and orderPR is colocated
   * with customerPR. <br>
   * getColocatedChildRegions(customerPR) will return List{orderPR}<br>
   * getColocatedChildRegions(orderPR) will return List{shipmentPR}<br>
   * getColocatedChildRegions(shipmentPR) will return empty List{}<br>
   * 
   * @param partitionedRegion
   * @return list of all child partitioned regions colocated with the region
   * @since 5.8Beta
   */
  public static List<PartitionedRegion> getColocatedChildRegions(
      PartitionedRegion partitionedRegion) {
    List<PartitionedRegion> colocatedChildRegions = new ArrayList<PartitionedRegion>();
    Region prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion
        .getCache());
    PartitionRegionConfig prConf = null;
   // final List allPRNamesList = new ArrayList(prRoot.keySet());
    Iterator itr = prRoot.keySet().iterator();
    while ( itr.hasNext()) {
      try {
        String prName = (String)itr.next();
        /*if (prName == prName) {
          continue;
        }*/
        try {
          prConf = (PartitionRegionConfig)prRoot.get(prName);
        }
        catch (EntryDestroyedException ede) {
          continue;
        }
        if (prConf == null) {
          // darrel says: I'm seeing an NPE in this code after pr->rem
          // merge so I added this check and continue
          continue;
        }
        int prID = prConf.getPRId();
        PartitionedRegion prRegion = PartitionedRegion.getPRFromId(prID);
        if (prRegion != null) {
          if (prRegion.getColocatedWith() != null) {
             if (prRegion.getColocatedWith().equals(
                partitionedRegion.getFullPath())
                || ("/" + prRegion.getColocatedWith()).equals(partitionedRegion
                    .getFullPath())) {
              // only regions directly colocatedWith partitionedRegion are
              // added to the list...
               prRegion.waitOnBucketMetadataInitialization();
               colocatedChildRegions.add(prRegion);
            }
          }
        }
      }
      catch (PRLocallyDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("PRLocallyDestroyedException : Region ={} is locally destroyed on this node", prConf.getPRId(), e);
        }
      }
      catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RegionDestroyedException : Region ={} is destroyed.", prConf.getPRId(), e);
        }
      }
    }

    //Fix for 44484 - Make the list of colocated child regions
    //is always in the same order on all nodes.
    Collections.sort(colocatedChildRegions, new Comparator<PartitionedRegion>() {
      @Override
      public int compare(PartitionedRegion o1, PartitionedRegion o2) {
        if(o1.isShadowPR() == o2.isShadowPR()) {
          return o1.getFullPath().compareTo(o2.getFullPath());
        }
        if(o1.isShadowPR()) {
          return 1;
        }
        return -1;
      }
    });
    return colocatedChildRegions;
  }
  
  //TODO why do we have this method here?
  public static Function getFunctionInstance(Serializable function) {
    Function functionInstance = null;
    if (function instanceof String) {
      functionInstance = FunctionService.getFunction((String)function);
      Assert.assertTrue(functionInstance != null, "Function " + function
          + " is not registered on this node ");
    }
    else {
      functionInstance = (Function)function;
    }
    return functionInstance;
  }

  public static PartitionedRegion getLeaderRegion(PartitionedRegion prRegion) {
    PartitionedRegion parentRegion;
    
    while((parentRegion = getColocatedRegion(prRegion)) != null) {
      prRegion = parentRegion;
    }
    
    return prRegion;
  }
  
  // Gemfirexd will skip initialization for PR, so just get region name without waitOnInitialize
  public static PartitionedRegion getLeaderRegionName(PartitionedRegion prRegion) {
    PartitionedRegion parentRegion;
    
    while((parentRegion = getColocatedRegionName(prRegion)) != null) {
      prRegion = parentRegion;
    } 
      
    return prRegion;
  }

  private static String getRegionIdentifier(String regionName) {
    if (regionName.startsWith("/")) {
      return regionName.replace("/", "#");
    }
    else {
      return "#" + regionName.replace("/", "#");
    }
  }

  /**
   * Test to see if there are any persistent child regions
   * of a partitioned region.
   */
  public static boolean hasPersistentChildRegion(
      PartitionedRegion region) {
    for(PartitionedRegion child : getColocatedChildRegions(region)) {
      if(child.getDataPolicy().withPersistence()) {
        return true;
      }
    }
    return false;
  }
}
