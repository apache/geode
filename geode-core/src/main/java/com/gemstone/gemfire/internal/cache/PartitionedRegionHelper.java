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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.partition.PartitionNotAvailableException;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 */
public class PartitionedRegionHelper
  {
  private static final Logger logger = LogService.getLogger();
  
  // ///////////// All the final variable //////////////////
  /** 1 MB */
  static final long BYTES_PER_MB = 1024 * 1024;

  /** Name of allPartitionedRegions Region * */
//  static final String PARTITIONED_REGION_CONFIG_NAME = "__Config";

  /** Prefix for the bucket2Node Region name defined in the global space. */
//  static final String BUCKET_2_NODE_TABLE_PREFIX = "_B2N_";

  /**
   * The administrative region used for storing Partitioned Region meta data sub
   * regions *
   */
  public static final String PR_ROOT_REGION_NAME = "__PR";

  /** Name of the DistributedLockService that PartitionedRegions used. */
  public static final String PARTITION_LOCK_SERVICE_NAME = "__PRLS";

  /** This is used to create bucket regions */
  static final String BUCKET_REGION_PREFIX = "_B_";

  /**
   * Time to wait for ownership (ms)
   * <p>
   * This should not be used normally. Internally, GemFire uses global locks to
   * modify shared meta-data and this property controls the delay before giving
   * up trying to acquire a global lock
   */
  static final String VM_OWNERSHIP_WAIT_TIME_PROPERTY = "gemfire.VM_OWNERSHIP_WAIT_TIME";

  /** Wait forever for ownership */
  static final long VM_OWNERSHIP_WAIT_TIME_DEFAULT = Long.MAX_VALUE;

  static final String MAX_PARTITIONED_REGION_ID = "MAX_PARTITIONED_REGION_ID";

  public static final int DEFAULT_WAIT_PER_RETRY_ITERATION = 100; // milliseconds

  public static final int DEFAULT_TOTAL_WAIT_RETRY_ITERATION = 
    60 * 60 * 1000
    ; // milliseconds

  public static final DataPolicy DEFAULT_DATA_POLICY = DataPolicy.PARTITION;

  public static final Set ALLOWED_DATA_POLICIES;
  
  static final Object dlockMonitor = new Object(); 

  static {
    Set policies = new HashSet();
    policies.add(DEFAULT_DATA_POLICY);
    policies.add(DataPolicy.PERSISTENT_PARTITION);
//    policies.add(DataPolicy.NORMAL);
    ALLOWED_DATA_POLICIES = Collections.unmodifiableSet(policies);
  }

  
  
  /** 
   * This function is used for cleaning the config meta data for the failed or closed 
   * PartitionedRegion node.
   * 
   * @param failedNode
   *          The failed PartitionedRegion Node
   * @param regionIdentifier
   *          The PartitionedRegion for which the cleanup is required
   * @param cache
   *          GemFire cache.
   */  
  static void removeGlobalMetadataForFailedNode(Node failedNode,
      String regionIdentifier, GemFireCacheImpl cache){
    removeGlobalMetadataForFailedNode(failedNode, regionIdentifier, cache, true);
  }
  
  /**
   * This function is used for cleaning the config meta data for the failed or closed 
   * PartitionedRegion node.
   * 
   * @param failedNode
   *          The failed PartitionedRegion Node
   * @param regionIdentifier
   *          The PartitionedRegion for which the cleanup is required
   * @param cache
   *          GemFire cache.
   * @param lock
   *          True if this removal should acquire and release the RegionLock
   */
  static void removeGlobalMetadataForFailedNode(Node failedNode,
      String regionIdentifier, GemFireCacheImpl cache, final boolean lock)
  {
    Region root = PartitionedRegionHelper.getPRRoot(cache, false);
    if (root == null) {
      return; // no partitioned region info to clean up
    }
//    Region allPartitionedRegions = PartitionedRegionHelper.getPRConfigRegion(
//        root, cache);
    PartitionRegionConfig prConfig = (PartitionRegionConfig)root
        .get(regionIdentifier);
    if (null == prConfig || !prConfig.containsNode(failedNode)) {
      return;
    }

    final PartitionedRegion.RegionLock rl = PartitionedRegion.getRegionLock(regionIdentifier, cache);
    try {
      if (lock) {
        rl.lock();
//        if (!rl.lock()) {
//          return;
//        }
      }
      prConfig = (PartitionRegionConfig)root.get(regionIdentifier);
      if ( prConfig != null  && prConfig.containsNode(failedNode)  ) {
        if(logger.isDebugEnabled()) {
          logger.debug("Cleaning up config for pr {} node {}", regionIdentifier, failedNode);
        }
        if ((prConfig.getNumberOfNodes() - 1) <= 0) {
          if(logger.isDebugEnabled()) {
            logger.debug("No nodes left but failed node {} destroying entry {} nodes {}",
                failedNode, regionIdentifier, prConfig.getNodes());
          }
          try {
            root.destroy(regionIdentifier);
          } catch (EntryNotFoundException e) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.PartitionedRegionHelper_GOT_ENTRYNOTFOUNDEXCEPTION_IN_DESTROY_OP_FOR_ALLPRREGION_KEY_0, 
                regionIdentifier), e);
          }
        }
        else {
          prConfig.removeNode(failedNode);
          if(prConfig.getNumberOfNodes() == 0) {
            root.destroy(regionIdentifier);
          } else {
            // We can't go backwards, or we potentially lose data
            root.put(regionIdentifier, prConfig);
          }
        }
      }
    }
    finally {
      if (lock) {      
        rl.unlock();
      }        
    }
  }

  /**
   * Return a region that is the root for all Partitioned Region metadata on this
   * node
   */
  public static LocalRegion getPRRoot(final Cache cache) {
    return getPRRoot(cache, true);
  }
  
  /**
   * Return a region that is the root for all PartitionedRegion meta data on
   * this Node. The main administrative Regions contained within are
   * <code>allPartitionedRegion</code> (Scope DISTRIBUTED_ACK) and
   * <code>bucket2Node</code> (Scope DISTRIBUTED_ACK) and dataStore regions.
   * 
   * @return a GLOBLAL scoped root region used for PartitionedRegion
   *         administration
   */
  public static LocalRegion getPRRoot(final Cache cache, boolean createIfAbsent)
  {
    GemFireCacheImpl gemCache = (GemFireCacheImpl) cache;
    DistributedRegion root = (DistributedRegion) gemCache.getRegion(PR_ROOT_REGION_NAME, true);
    if (root == null) {
      if (!createIfAbsent) {
        return null;
      }
      if(logger.isDebugEnabled()) {
        logger.debug("Creating root Partitioned Admin Region {}", PartitionedRegionHelper.PR_ROOT_REGION_NAME);
      }
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.addCacheListener(new FixedPartitionAttributesListener());
      if (Boolean.getBoolean("gemfire.PRDebug")) {
        factory.addCacheListener( new CacheListenerAdapter() {
          @Override
          public void afterCreate(EntryEvent event)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("Create Event for allPR: key = {} oldVal = {} newVal = {} Op = {} origin = {} isNetSearch = {}",
                  event.getKey(), event.getOldValue(), event.getNewValue(), event.getOperation(), event.getDistributedMember(),
                  event.getOperation().isNetSearch());
            }
          }

          @Override
          public void afterUpdate(EntryEvent event)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("Update Event for allPR: key = {} oldVal = {} newVal = {} Op = {} origin = {} isNetSearch = {}",
                  event.getKey(), event.getOldValue(), event.getNewValue(), event.getOperation(), event.getDistributedMember(),
                  event.getOperation().isNetSearch());
            }
          }

          @Override
          public void afterDestroy(EntryEvent event)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("Destroy Event for allPR: key = {} oldVal = {} newVal = {} Op = {} origin = {} isNetSearch = {}",
                  event.getKey(), event.getOldValue(), event.getNewValue(), event.getOperation(), event.getDistributedMember(),
                  event.getOperation().isNetSearch());
            }
          }
        });
        
        factory.setCacheWriter(new CacheWriterAdapter() {
          @Override
          public void beforeUpdate(EntryEvent event) throws CacheWriterException
          {
            // the prConfig node list must advance (otherwise meta data becomes out of sync)
            final PartitionRegionConfig newConf = (PartitionRegionConfig) event.getNewValue();
            final PartitionRegionConfig oldConf = (PartitionRegionConfig) event.getOldValue();
            if (newConf != oldConf &&            
                ! newConf.isGreaterNodeListVersion(oldConf) ) {
              throw new CacheWriterException(LocalizedStrings.PartitionedRegionHelper_NEW_PARTITIONEDREGIONCONFIG_0_DOES_NOT_HAVE_NEWER_VERSION_THAN_PREVIOUS_1.toLocalizedString(new Object[] {newConf, oldConf}));
            }
          }
        });
      }

      RegionAttributes ra = factory.create();
      // Create anonymous stats holder for Partitioned Region meta data
      final HasCachePerfStats prMetaStatsHolder = new HasCachePerfStats() {
        public CachePerfStats getCachePerfStats()
        {
          return new CachePerfStats(cache.getDistributedSystem(), "partitionMetaData");
        }
      };

      try {
        root = (DistributedRegion) gemCache.createVMRegion(PR_ROOT_REGION_NAME, ra, 
            new InternalRegionArguments()
            .setIsUsedForPartitionedRegionAdmin(true)
            .setCachePerfStatsHolder(prMetaStatsHolder));
        root.getDistributionAdvisor().addMembershipListener(new MemberFailureListener());
      }
      catch (RegionExistsException silly) {
        // we avoid this before hand, but yet we have to catch it
        root = (DistributedRegion) gemCache.getRegion(PR_ROOT_REGION_NAME, true);
      } catch (IOException ieo) {
        Assert.assertTrue(false, "IOException creating Partitioned Region root: " + ieo);
      } catch (ClassNotFoundException cne) {
        Assert.assertTrue(false, "ClassNotFoundExcpetion creating Partitioned Region root: " + cne);
      }
    }
    Assert.assertTrue(root!=null, "Can not obtain internal Partitioned Region configuration root");
    return root;
  }
  
//TODO rebalancing - this code was added here in the merge of -r22804:23093 from trunk
  //because of changes made on trunk that require this method, which was removed on
  //prRebalancing. It probably needs refactoring.
  //The idea here is to remove meta data from the partitioned region for a node that
  //has left the cache. 
  //A couple options that didn't work
  //   - remove metadata in region advisor for PR instead - this doesn't work because 
  //the a member can close it's cache and then recreate the same region. Another member
  //might end up removing meta data after the region is recreated, leading to inconsistent metadata
  // - remove metadata on cache closure in the member that is closing - This didn't work because
  //we can't do region operations after isClosing is set to true (to remove metadata). Removing metadata
  //before is closing is set to true results operations being silently ignored because of inconsistent metadata
  //and regions.
  /**
   * Clean the config meta data for a DistributedMember which has left the
   * DistributedSystem, one PartitionedRegion at a time.
   */
  public static void cleanUpMetaDataOnNodeFailure(DistributedMember failedMemId)
  {
    try {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache == null || cache.getCancelCriterion().cancelInProgress() != null) {
        return;
      }

      DM dm = cache.getDistributedSystem().getDistributionManager(); 

      if (logger.isDebugEnabled()) {
        logger.debug("Cleaning PartitionedRegion meta data for memberId={}", failedMemId);
      }
      Region rootReg = PartitionedRegionHelper.getPRRoot(cache, false);
      if (rootReg == null) {
        return;
      }

      final ArrayList<String> ks = new ArrayList<String>(rootReg.keySet());
      if (ks.size() > 1) {
        Collections.shuffle(ks, PartitionedRegion.rand);
      }
      for (String prName : ks ) {
        try {
          cleanUpMetaDataForRegion(cache, prName, failedMemId, null);

        } catch (CancelException e) {
          // okay to ignore this - metadata will be cleaned up by cache close operation
        } catch (Exception e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Got exception in cleaning up metadata. {}", e.getMessage(), e);
          }
        }
      }
    } catch(CancelException e) {
      //ignore
    }
  }
  
  public static void cleanUpMetaDataForRegion(final GemFireCacheImpl cache,
      final String prName, final DistributedMember failedMemId,
      final Runnable postCleanupTask) {
    boolean runPostCleanUp = true; 
    try {
    final PartitionRegionConfig prConf;
    Region rootReg = PartitionedRegionHelper.getPRRoot(cache, false);
    if (rootReg == null) {
      return;
    }
    try {
      prConf = (PartitionRegionConfig)rootReg.get(prName);
    }
    catch (EntryDestroyedException ede) {
      return;
    }
    if (prConf == null) {
      // darrel says: I'm seeing an NPE in this code after pr->rem
      // merge
      // so I added this check and continue
      return;
    }
    Set<Node> nodeList = prConf.getNodes();
    if (nodeList == null) {
      return;
    }

    for (final Node node1 : nodeList) {
      if (cache.getCancelCriterion().cancelInProgress() != null) {
        return;
      }
      if (node1.getMemberId().equals(failedMemId)) {
        //Do the cleanup in another thread so we don't have the advisor locked.
        //Fix for #45365, we don't schedule an asynchronous task until
        //we have determined the node to remove (Which includes the 
        //serial number).
        cache.getDistributionManager().getPrMetaDataCleanupThreadPool().execute(new Runnable() {
          public void run() {
            cleanPartitionedRegionMetaDataForNode(cache,
                node1, prConf, prName);
            if(postCleanupTask != null) {
              postCleanupTask.run();
            }
          }
        });
        runPostCleanUp = false;
        return;
      }
    }
    } finally {
      if(runPostCleanUp && postCleanupTask != null) {
        postCleanupTask.run();
      }
    }
  }
  
  /**
   * This is a function for cleaning the config meta data (both the
   * configuration data and the buckets) for a Node that hosted a
   * PartitionedRegion
   */
  private static void cleanPartitionedRegionMetaDataForNode(GemFireCacheImpl cache, Node node,
      PartitionRegionConfig prConf, String regionIdentifier)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Cleaning PartitionedRegion meta data for node={} for Partitioned Region={} configuration={}",
          node, regionIdentifier, prConf);
    }
    PartitionedRegionHelper.removeGlobalMetadataForFailedNode(node, regionIdentifier,
        cache);
    
    if (logger.isDebugEnabled()) {
      logger.debug("Done Cleaning PartitionedRegion meta data for memberId={} for {}", node, regionIdentifier);
    }
  }

  /**
   * Runs hashCode() on given key producing a long value and then finds absolute
   * value of the modulus with bucketSize. For better key distribution, possibly
   * use MD5 or SHA or any unique ID generator for the hash function.
   * 
   * @param pr
   *          the partitioned region on which to operate
   * @param key
   *          the key on which to determine the hash key
   * @return the bucket id the key hashes to
   */
  // private static int NOSIGN = 0x7fffffff; 
 /* public static int getHashKey(PartitionedObject key)
  {
    PartitionedRegion pRegion = (PartitionedRegion)entryOp.getRegion();
    RoutingResolver resolver = pRegion.getRoutingResolver();
    
    int totalNumberOfBuckets = pRegion.getTotalNumberOfBuckets();
    Object resolveKey = null;
    if (resolver == null) {
      resolveKey = key;
    } else {
    	
      //resolveKey = resolver.getPartitionKey(key);
    	resolveKey = resolver.getRoutingObject((EntryOperation)key);
    }
    int hc = resolveKey.hashCode();
    int bucketId = hc % totalNumberOfBuckets;
    // Force positive bucket ids only
    return Math.abs(bucketId);
    // We should use the same hash code spreader as most other java.util hash tables.
//    h += ~(h << 9);
//    h ^=  (h >>> 14);
//    h +=  (h << 4);
//    h ^=  (h >>> 10);
//    h &= NOSIGN;
//    return h % totalNumberOfBuckets;
  }
**/

  static private PartitionResolver getResolver(PartitionedRegion pr,
      Object key, Object callbackArgument) {
    // First choice is one associated with the region
    PartitionResolver result = pr.getPartitionResolver();
    if (result != null) {
      return result;
    }
    
    // Second is the key
    if (key != null  && key instanceof PartitionResolver) {
      return (PartitionResolver)key;
    }
    
    // Third is the callback argument
    if (callbackArgument != null
        && callbackArgument instanceof PartitionResolver) {
      return (PartitionResolver)callbackArgument;
    }
    
    // There is no resolver.
    return null;
  }

  /**
   * Runs hashCode() on given key/routing object producing a long value and then
   * finds absolute value of the modulus with bucketSize. For better key
   * distribution, possibly use MD5 or SHA or any unique ID generator for the
   * hash function.
   * 
   * @param pr
   *                the partitioned region on which to operate
   * @param operation
   *                operation
   * @param key
   *                the key on which to determine the hash key
   * @param callbackArgument
   *                the callbackArgument is passed to
   *                <code>PartitionResolver</code> to get Routing object
   * @return the bucket id the key/routing object hashes to
   */
  public static int getHashKey(PartitionedRegion pr, Operation operation,
      Object key, Object value, Object callbackArgument) {
    // avoid creating EntryOperation if there is no resolver
    try {
      return getHashKey(null, pr, operation, key, value, callbackArgument);
    } catch (IllegalStateException e) { // bug #43651 - check for shutdown before throwing this
      pr.getCache().getCancelCriterion().checkCancelInProgress(e);
      throw e;
    }
  }

  /**
   * Runs hashCode() on given key/routing object producing a long value and then
   * finds absolute value of the modulus with bucketSize. For better key
   * distribution, possibly use MD5 or SHA or any unique ID generator for the
   * hash function.
   * 
   * @param event
   *                entry event created for this entry operation
   * @return the bucket id the key/routing object hashes to
   */
  public static int getHashKey(EntryOperation event) {
    return getHashKey(event, null, null, null, null, null);
  }

  /**
   * Runs hashCode() on given key/routing object producing a long value and then
   * finds absolute value of the modulus with bucketSize. For better key
   * distribution, possibly use MD5 or SHA or any unique ID generator for the
   * hash function.
   * 
   * @param event
   *                entry event created for this entry operation; can be null
   * @param pr
   *                the partitioned region on which to operate
   * @param operation
   *                operation
   * @param key
   *                the key on which to determine the hash key
   * @param callbackArgument
   *                the callbackArgument is passed to
   *                <code>PartitionResolver</code> to get Routing object
   * @return the bucket id the key/routing object hashes to
   */
  private static int getHashKey(EntryOperation event, PartitionedRegion pr,
      Operation operation, Object key, Object value, Object callbackArgument) {
    // avoid creating EntryOperation if there is no resolver
    if (event != null) {
      pr = (PartitionedRegion)event.getRegion();
      key = event.getKey();
      callbackArgument = event.getCallbackArgument();
    }

    PartitionResolver resolver = getResolver(pr, key, callbackArgument);
    Object resolveKey = null;
    if (pr.isFixedPartitionedRegion()) {
      String partition = null ;
      if (resolver instanceof FixedPartitionResolver) {
        Map<String, Integer[]> partitionMap = pr.getPartitionsMap();
        if (event == null) {
          event = new EntryOperationImpl(pr, operation, key, value,
              callbackArgument);
        }
        partition = ((FixedPartitionResolver)resolver).getPartitionName(
            event, partitionMap.keySet());
        if (partition == null) {
          Object[] prms = new Object[] { pr.getName(), resolver };
          throw new IllegalStateException(
              LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_PARTITIONRESOLVER_1_RETURNED_PARTITION_NAME_NULL
                  .toLocalizedString(prms));
        }
        Integer[] bucketArray = partitionMap.get(partition);
        if (bucketArray == null) {
          Object[] prms = new Object[] { pr.getName(), partition };
          throw new PartitionNotAvailableException(
              LocalizedStrings.PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_1_IS_NOT_AVAILABLE_ON_ANY_DATASTORE
                  .toLocalizedString(prms));
        }
        int numBukets = bucketArray[1];
        resolveKey = (numBukets == 1) ? partition : resolver.getRoutingObject(event);
      }
      else if (resolver == null) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_RESOLVER_IS_NOT_AVAILABLE
                .toString(pr.getName()));
      }
      else if (!(resolver instanceof FixedPartitionResolver)) {
        Object[] prms = new Object[] { pr.getName(), resolver };
        throw new IllegalStateException(
            LocalizedStrings.PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_RESOLVER_DEFINED_1_IS_NOT_AN_INSTANCE_OF_FIXEDPARTITIONRESOLVER
                .toLocalizedString(prms));
      }
      return assignFixedBucketId(pr, partition, resolveKey);
    }
    else {
      // Calculate resolveKey.
      if (resolver == null) {
        // no custom partitioning at all
        resolveKey = key;
        if (resolveKey == null) {
          throw new IllegalStateException("attempting to hash null");
        }
      }
      else {
        if (event == null) {
          event = new EntryOperationImpl(pr, operation, key, value,
              callbackArgument);
        }
        resolveKey = resolver.getRoutingObject(event);
        if (resolveKey == null) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionedRegionHelper_THE_ROUTINGOBJECT_RETURNED_BY_PARTITIONRESOLVER_IS_NULL
                  .toLocalizedString());
        }
      }
      // Finally, calculate the hash.
      return getHashKey(pr, resolveKey);
    }
  }

  private static int assignFixedBucketId(PartitionedRegion pr,
      String partition, Object resolveKey) {
    int startingBucketID = 0;
    int partitionNumBuckets = 0;
    boolean isPartitionAvailable = pr.getPartitionsMap().containsKey(partition);
    Integer[] partitionDeatils = pr.getPartitionsMap().get(partition);
    if (isPartitionAvailable) {
      startingBucketID = partitionDeatils[0];
      partitionNumBuckets = partitionDeatils[1];

      int hc = resolveKey.hashCode();
      int bucketId = Math.abs(hc % partitionNumBuckets);
      int partitionBucketID = bucketId + startingBucketID;
      assert partitionBucketID != KeyInfo.UNKNOWN_BUCKET;
      return partitionBucketID;
    }
    List<FixedPartitionAttributesImpl> localFPAs = pr
        .getFixedPartitionAttributesImpl();
    
    if (localFPAs != null) {
      for (FixedPartitionAttributesImpl fpa : localFPAs) {
        if (fpa.getPartitionName().equals(partition)) {
          isPartitionAvailable = true;
          partitionNumBuckets = fpa.getNumBuckets();
          startingBucketID = fpa.getStartingBucketID();
          break;
        }
      }
    }

    if (!isPartitionAvailable) {
      List<FixedPartitionAttributesImpl> remoteFPAs = pr.getRegionAdvisor()
          .adviseAllFixedPartitionAttributes();
      for (FixedPartitionAttributesImpl fpa : remoteFPAs) {
        if (fpa.getPartitionName().equals(partition)) {
          isPartitionAvailable = true;
          partitionNumBuckets = fpa.getNumBuckets();
          startingBucketID = fpa.getStartingBucketID();
          break;
        }
      }
    }

    if (partitionNumBuckets == 0) {
      if (isPartitionAvailable) {
        Object[] prms = new Object[] { pr.getName(), partition };
        throw new IllegalStateException(
            LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_FOR_PARTITION_1_PARTITIION_NUM_BUCKETS_ARE_SET_TO_0_BUCKETS_CANNOT_BE_CREATED_ON_THIS_MEMBER
                .toLocalizedString(prms));
      }
    }

    if (!isPartitionAvailable) {
      Object[] prms = new Object[] { pr.getName(), partition };
      throw new PartitionNotAvailableException(
          LocalizedStrings.PartitionedRegionHelper_FOR_REGION_0_PARTITION_NAME_1_IS_NOT_AVAILABLE_ON_ANY_DATASTORE
              .toLocalizedString(prms));
    }
    int hc = resolveKey.hashCode();
    int bucketId = Math.abs(hc % partitionNumBuckets);
    int partitionBucketID = bucketId + startingBucketID;
    assert partitionBucketID != KeyInfo.UNKNOWN_BUCKET;
    return partitionBucketID;
  }
  
  public static int getHashKey(PartitionedRegion pr, Object routingObject) {
    return getHashKey(routingObject, pr.getTotalNumberOfBuckets());
  }

  public static int getHashKey(Object routingObject, int totalNumBuckets) {
    int hc = routingObject.hashCode();
    int bucketId = hc % totalNumBuckets;
    // Force positive bucket ids only
    return Math.abs(bucketId);
  }

  public static PartitionedRegion getPartitionedRegion(String prName,
      Cache cache)
  {
    Region region = cache.getRegion(prName);
    if (region != null) {
      if (region instanceof PartitionedRegion)
        return (PartitionedRegion)region;
    }
    return null;
  }

  public static boolean isBucketRegion(String fullPath) {
    return getBucketName(fullPath) != null;
  }
  
  /** 
   * Find a ProxyBucketRegion by parsing the region fullPath
   * @param cache 
   * @param fullPath full region path to parse
   * @param postInit true if caller should wait for bucket initialization to complete
   * @return ProxyBucketRegion as Bucket or null if not found
   * @throws PRLocallyDestroyedException if the PartitionRegion is locally destroyed
   */
  public static Bucket getProxyBucketRegion(Cache cache, String fullPath, boolean postInit) throws PRLocallyDestroyedException {
    if(cache == null) {
      //No cache
      return null;
    }
    // fullPath = /__PR/_B_1_10
    String bucketName = getBucketName(fullPath);
    if (bucketName == null) {
      return null;
    }
    
    String prid = getPRPath(bucketName);
//    PartitionedRegion region = 
//        PartitionedRegion.getPRFromId(Integer.parseInt(prid));
    
    Region region;
    int oldLevel =         // Set thread local flag to allow entrance through initialization Latch
      LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
    region = cache.getRegion(prid);
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    if (region == null || !(region instanceof PartitionedRegion)) {
      return null;
    }
    
    PartitionedRegion pr = (PartitionedRegion) region; 
    
    int bid = getBucketId(bucketName);
    RegionAdvisor ra = (RegionAdvisor) pr.getDistributionAdvisor();
    if (postInit) {
      return ra.getBucketPostInit(bid);
    }
    else if ( ! ra.areBucketsInitialized()) {  
      // While the RegionAdvisor may be available, it's bucket meta-data may not be constructed yet
      return null;
    } 
    else {
      return ra.getBucket(bid);      
    }

  }
  
  private final static String BUCKET_FULL_PATH_PREFIX = PR_ROOT_REGION_NAME + Region.SEPARATOR + BUCKET_REGION_PREFIX;  
  /** 
   * Get the bucket string by parsing the region fullPath
   * @param bucketFullPath full region path to parse
   * @return the bucket string or null if no bucket string is present
   */
  public static String getBucketName(String bucketFullPath) {
    if (bucketFullPath == null || bucketFullPath.length() == 0) {
      return null;
    }
    int idxStartRoot = bucketFullPath.indexOf(BUCKET_FULL_PATH_PREFIX);
    // parse bucketString
    if (idxStartRoot != -1) {
      int idxEndRoot = idxStartRoot + PR_ROOT_REGION_NAME.length() + Region.SEPARATOR.length();
      return bucketFullPath.substring(idxEndRoot);
    }

    DistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("getBucketString no match fullPath={}", bucketFullPath);
      }
    }
    return null;
  }
  
  public static String getBucketFullPath(String prFullPath, int bucketId) {
    String name = getBucketName(prFullPath, bucketId);
    if (name != null)
      return Region.SEPARATOR + PR_ROOT_REGION_NAME + Region.SEPARATOR + name;
    
    return null;
    
  }
  
  public static String escapePRPath(String prFullPath) {
    String escaped = prFullPath.replace("_", "__");
    escaped = escaped.replace(LocalRegion.SEPARATOR_CHAR, '_');
    return escaped;
  }
  
  
  public static String TWO_SEPARATORS = LocalRegion.SEPARATOR + LocalRegion.SEPARATOR;
  
  public static String unescapePRPath(String escapedPath) {
    String path = escapedPath.replace('_', LocalRegion.SEPARATOR_CHAR);
    path = path.replace(TWO_SEPARATORS, "_");
    return path;
  }
  
  public static String getBucketName(String prPath, int bucketId) {
    return PartitionedRegionHelper.BUCKET_REGION_PREFIX
    + PartitionedRegionHelper.escapePRPath(prPath)
    + PartitionedRegion.BUCKET_NAME_SEPARATOR + bucketId;
  }


  /**
   * Returns the PR name give the bucketName (see getBucketName).
   */
  public static String getPRPath(String bucketName) {
    // bucketName = _B_PRNAME_10
    int pridIdx = 
        PartitionedRegionHelper.BUCKET_REGION_PREFIX.length();
    int bidSepIdx = 
        bucketName.lastIndexOf(PartitionedRegion.BUCKET_NAME_SEPARATOR);
    Assert.assertTrue(bidSepIdx > -1, 
                      "getProxyBucketRegion failed on " + bucketName);
    return unescapePRPath(bucketName.substring(pridIdx, bidSepIdx));
  }
  /**
   * Returns the bucket id gvien the bucketName (see getBucketName).
   */
  public static int getBucketId(String bucketName) {
    // bucketName = _B_PRNAME_10
    int bidSepIdx = 
        bucketName.lastIndexOf(PartitionedRegion.BUCKET_NAME_SEPARATOR);
    String bid = bucketName.substring(bidSepIdx+1);
    return Integer.parseInt(bid);
  }
    
  /**
   * This method returns true if the last region in provided fullPath is a
   * sub-region else it returns false. If fullPath is "/REGION1" it would return
   * false and if it is "/REGION1/REGION2", it would return true, which means
   * that Region2 is a sub-region.
   * 
   * @param fullPath
   *          full path of the region
   * @return true if given full path has sub-regions else return false
   */

  public static boolean isSubRegion(String fullPath)
  {
    boolean isSubRegion = false;
    if (null != fullPath) {
      int idx = fullPath.indexOf(Region.SEPARATOR, Region.SEPARATOR.length());
      if (idx >= 0)
        isSubRegion = true;
    }
    return isSubRegion;
  }
  
  /**
   * This method returns true if the member is found in the membership list of
   * this member, else false. 
   * @param mem
   * @param cache
   * @return true if mem is found in membership list of this member.
   */
  public static boolean isMemberAlive(DistributedMember mem, GemFireCacheImpl cache) {
    return getMembershipSet(cache).contains(mem);
  }  
  
  /**
   * Returns the current membership Set for this member.
   * @param cache
   * @return membership Set.
   */
  public static Set getMembershipSet(GemFireCacheImpl cache) {
    return cache.getDistributedSystem().getDistributionManager()
        .getDistributionManagerIds();
  }

  /**
   * Utility method to print warning when nodeList in b2n region is found empty.
   * This will signify potential data loss scenario.
   * @param partitionedRegion
   * @param bucketId Id of Bucket whose nodeList in b2n is empty.
   * @param callingMethod methodName of the calling method.
   */
  public static void logForDataLoss(PartitionedRegion partitionedRegion, int bucketId, String callingMethod) {
    if (! Boolean.getBoolean("gemfire.PRDebug")) {
      return;
    }
    Region root = PartitionedRegionHelper.getPRRoot(partitionedRegion.getCache());
//    Region allPartitionedRegions = PartitionedRegionHelper.getPRConfigRegion(
//        root, partitionedRegion.getCache());
    PartitionRegionConfig prConfig = (PartitionRegionConfig)root
        .get(partitionedRegion.getRegionIdentifier());
    if( prConfig == null )
    	return;
    
    Set members = partitionedRegion.getDistributionManager()
    .getDistributionManagerIds(); 
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.PartitionedRegionHelper_DATALOSS___0____SIZE_OF_NODELIST_AFTER_VERIFYBUCKETNODES_FOR_BUKID___1__IS_0,
        new Object[] {callingMethod, Integer.valueOf(bucketId)}));
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.PartitionedRegionHelper_DATALOSS___0____NODELIST_FROM_PRCONFIG___1,
        new Object[] {callingMethod, printCollection(prConfig.getNodes())})); 
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.PartitionedRegionHelper_DATALOSS___0____CURRENT_MEMBERSHIP_LIST___1,
        new Object[] {callingMethod, printCollection(members)}));
  }
  
  /**
   * Utility method to print a collection.
   * @param c
   * @return String
   */
  public static String printCollection(Collection c) {
    if (c != null) {
      StringBuffer sb = new StringBuffer("[");
      Iterator itr = c.iterator();
      while(itr.hasNext()) {
        sb.append(itr.next());
        if (itr.hasNext()) {
          sb.append(", ");
        }
      }
      sb.append("]");
      return sb.toString();
    } else {
      return "[null]";
    }
  }
  
  /**
   * Destroys and removes the distributed lock service.
   * This is called from cache closure operation.
   * 
   * @see PartitionedRegion#afterRegionsClosedByCacheClose(GemFireCacheImpl)
   */
  static void destroyLockService() {
    DistributedLockService dls = null;
     synchronized (dlockMonitor) {
     	dls = DistributedLockService
    	.getServiceNamed(PARTITION_LOCK_SERVICE_NAME);
     }
     if(dls != null) {  
     		try {
     	         DistributedLockService.destroy(PARTITION_LOCK_SERVICE_NAME);
     	       } catch (IllegalArgumentException ex) {
     	         // Our dlockService is already destroyed,
     	         // probably by another thread - ignore     	  
     	       }
     	}
     }

  public static boolean isBucketPrimary(Bucket buk) {
    return buk.getBucketAdvisor().isPrimary();
  }
  
  public static boolean isRemotePrimaryAvailable(PartitionedRegion region,
      FixedPartitionAttributesImpl fpa) {
    List<FixedPartitionAttributesImpl> fpaList = region.getRegionAdvisor()
        .adviseSameFPAs(fpa);
    
    for (FixedPartitionAttributes remotefpa : fpaList) {
      if (remotefpa.isPrimary()) {
        return true;
      }
    }
    return false;
  }

  public static FixedPartitionAttributesImpl getFixedPartitionAttributesForBucket(
      PartitionedRegion pr, int bucketId) {
    List<FixedPartitionAttributesImpl> localFPAs = pr
        .getFixedPartitionAttributesImpl();

    if (localFPAs != null) {
      for (FixedPartitionAttributesImpl fpa : localFPAs) {
        if (fpa.hasBucket(bucketId)) {
          return fpa;
        }
      }
    }

    List<FixedPartitionAttributesImpl> remoteFPAs = pr.getRegionAdvisor()
        .adviseAllFixedPartitionAttributes();
    for (FixedPartitionAttributesImpl fpa : remoteFPAs) {
      if (fpa.hasBucket(bucketId)) {
        return fpa;
      }
    }
    Object[] prms = new Object[] { pr.getName(), Integer.valueOf(bucketId) };
    throw new PartitionNotAvailableException(
        LocalizedStrings.PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_IS_NOT_AVAILABLE_FOR_BUCKET_1_ON_ANY_DATASTORE
            .toLocalizedString(prms));
  }

  private static Set<String> getAllAvailablePartitions(PartitionedRegion region) {
    Set<String> partitionSet = new HashSet<String>();
    List<FixedPartitionAttributesImpl> localFPAs = region
        .getFixedPartitionAttributesImpl();
    if (localFPAs != null) {
      for (FixedPartitionAttributesImpl fpa : localFPAs) {
        partitionSet.add(fpa.getPartitionName());
      }
    }

    List<FixedPartitionAttributesImpl> remoteFPAs = region.getRegionAdvisor()
        .adviseAllFixedPartitionAttributes();
    for (FixedPartitionAttributes fpa : remoteFPAs) {
      partitionSet.add(fpa.getPartitionName());
    }
    return Collections.unmodifiableSet(partitionSet);
  }
  
  public static Set<FixedPartitionAttributes> getAllFixedPartitionAttributes(PartitionedRegion region) {
    Set<FixedPartitionAttributes> fpaSet = new HashSet<FixedPartitionAttributes>();
    List<FixedPartitionAttributesImpl> localFPAs = region
        .getFixedPartitionAttributesImpl();
    if (localFPAs != null) {
      fpaSet.addAll(localFPAs);
    }
    List<FixedPartitionAttributesImpl> remoteFPAs = region.getRegionAdvisor()
        .adviseAllFixedPartitionAttributes();
    fpaSet.addAll(remoteFPAs);
    return fpaSet;
  }
  
  private static class MemberFailureListener implements MembershipListener {

    public void memberJoined(InternalDistributedMember id) {
      
    }

    public void memberDeparted(final InternalDistributedMember id, boolean crashed) {
      PartitionedRegionHelper.cleanUpMetaDataOnNodeFailure(id);
    }

    public void memberSuspect(InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
    }
    
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    }
    
  }
}

class FixedPartitionAttributesListener extends CacheListenerAdapter {
  private static final Logger logger = LogService.getLogger();

  public void afterCreate(EntryEvent event) {
    PartitionRegionConfig prConfig = (PartitionRegionConfig)event.getNewValue();
    if (!prConfig.getElderFPAs().isEmpty()) {
      updatePartitionMap(prConfig);
    }
  }

  public void afterUpdate(EntryEvent event) {
    PartitionRegionConfig prConfig = (PartitionRegionConfig)event.getNewValue();
    if (!prConfig.getElderFPAs().isEmpty()) {
      updatePartitionMap(prConfig);
    }
  }

  private void updatePartitionMap(PartitionRegionConfig prConfig) {
    int prId = prConfig.getPRId();
    PartitionedRegion pr = null;
    
    try {
      pr = PartitionedRegion.getPRFromId(prId);
      if (pr != null) {
        Map<String, Integer[]> partitionMap = pr.getPartitionsMap();
        for (FixedPartitionAttributesImpl fxPrAttr : prConfig.getElderFPAs()) {
          partitionMap.put(fxPrAttr.getPartitionName(), new Integer[] {
              fxPrAttr.getStartingBucketID(), fxPrAttr.getNumBuckets() });
        }
      }
    } catch (PRLocallyDestroyedException e) {
      logger.debug("PRLocallyDestroyedException : Region ={} is locally destroyed on this node", prConfig.getPRId(), e);
    }
  }
}
