/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.cache.partition;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RecoveryLock;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketResponse;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An utility class to manage partitions (aka buckets) on a Partitioned Region
 * without requiring data (e.g. keys).
 * 
 * Note : Please contact support@gemstone.com before using these APIs.
 * 
 * <br>
 * This is an example of how this API can be used to create a view
 * region with redundancy 0, which is colocated with another region of
 * redundancy 1, without using the standard gemfire colocated-with attribute,
 * which only supports colocated regions with the same redundancy level.
 * 
 * Note that when using this API, the data in the view region is discarded every
 * time a primary moves.
 * 
 * <pre>
 * public class ColocatingPartitionListener implements PartitionListener,
 *     Declarable {
 *   private Cache cache;
 * 
 *   private List&lt;String&gt; viewRegionNames = new ArrayList&lt;String&gt;();
 * 
 *   public ColocatingPartitionListener() {
 *   }
 * 
 *   public void afterPrimary(int bucketId) { 
 * 
 *     for (String viewRegionName : viewRegionNames) {
 *       Region viewRegion = cache.getRegion(viewRegionName);
 *       PartitionManager.createPrimaryBucket(viewRegion, bucketId, true, true);
 *     }
 *   }
 * 
 *   public void init(Properties props) {
 *     String viewRegions = props.getProperty(&quot;viewRegions&quot;);
 *     StringTokenizer tokenizer = new StringTokenizer(viewRegions, &quot;,&quot;);
 *     while (tokenizer.hasMoreTokens()) {
 *       viewRegionNames.add(tokenizer.nextToken());
 *     }
 *   }
 * 
 *   public void afterRegionCreate(Region&lt;?, ?&gt; region) {
 *     cache = region.getCache();
 *   }
 * }
 * </pre>
 * 
 * In the declaration of the parent region in cache.xml, install 
 * the ColocatedPartitionListener as follows :<br>
 * 
 * <pre>
 * &lt;partition-attributes redundant-copies=&quot;1&quot;&gt;
 *     &lt;partition-listener&gt;
 *         &lt;class-name&gt;com.myCompany.ColocatingPartitionListener&lt;/class-name&gt;
 *          &lt;parameter name=&quot;viewRegions&quot;&gt;
 *              &lt;string&gt;/customer/ViewA,/customer/ViewB&lt;/string&gt;
 *          &lt;/parameter&gt;             
 *     &lt;/partition-listener&gt;
 * &lt;/partition-attributes&gt;
 * </pre>
 * 
 * If the regions needs to be rebalanced, use the {@link RebalanceFactory#excludeRegions(Set)}
 * method to exclude the view regions.
 * 
 * 
 * @author Yogesh Mahajan
 * 
 * @since 6.5
 */
public final class PartitionManager {
  private static final Logger logger = LogService.getLogger();
  
  private PartitionManager() {
  }

  /**
   * This method creates a copy of the bucket on the current node, if no
   * copy already exists. Depending on the values of destroyExistingLocal 
   * and destroyExistingRemote, it will first destroy the existing primary
   * copy of the bucket.
   * 
   * This behavior of this method is undefined on partitioned regions with
   * redundancy greater than 0. The behavior is also undefined for partitioned 
   * regions colocated with the colocated-with attribute.
   * 
   * This method creates primary bucket in the following way:
   * 
   * <p>
   * If the partitioned region does not have a primary bucket for the bucketId,
   * it creates a primary bucket on the member and returns true.
   * 
   * <p>
   * If the partitioned region does have a primary bucket for the bucketId on
   * the member :<br>
   * a) If destroyExistingLocal passed is true, it destroys the existing bucket,
   * and then creates a new primary bucket and returns true. <br>
   * b) If destroyExistingLocal passed is false, it does nothing and returns 
   * false.
   * 
   * <p>
   * If the partitioned region does have a primary bucket for the bucketId on
   * remote members :<br> 
   * a) If destroyExistingRemote passed is true, it destroys the existing bucket
   * on remote member, and then creates a new primary bucket on this member and
   * returns true. <br>
   * b) If destroyExistingRemote passed is false, it throws
   * IllegalStateException.
   * 
   * 
   * @param region
   *          the partitioned region on which to create the bucket
   * @param bucketId
   *          the identifier of the bucket to create
   * @param destroyExistingRemote
   *          whether to destroy the remote bucket if it exists
   * @param destroyExistingLocal
   *          whether to destroy the local bucket if it exists
   * @return true when the bucket has been created otherwise false
   * @throws IllegalArgumentException
   *           if the provided region is not a partitioned region
   * @throws IllegalArgumentException
   *           if the provided bucketId is less than zero or greater than or
   *           equal to the partitioned region's total number of buckets
   * @throws IllegalStateException
   *           if the partitioned region has the primary bucket for the bucket
   *           id on a remote member and the destroyExistingRemote parameter 
   *           provided is false
   */
  public static boolean createPrimaryBucket(final Region<?, ?> region,
      final int bucketId, final boolean destroyExistingRemote,
      final boolean destroyExistingLocal) {
    final PartitionedRegion pr = prCheck(region, bucketId);
    boolean createdBucket = false;
    
    //Getting this lock prevents rebalancing while this bucket is created.
    //It will also prevent conflicts with other members in createPrimaryBucket
    final RecoveryLock recovLock = pr.getRecoveryLock();
    recovLock.lock();
    try {
      DistributedMember primary = PartitionManager.getPrimaryMemberForBucket(pr,
          bucketId);
      InternalDistributedMember self = (InternalDistributedMember)pr.getCache()
      .getDistributedSystem().getDistributedMember();

      if (primary == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("createPrimaryBucket: {} bucket {} no existing primary, creating primary", pr, bucketId);
        }
        createdBucket = createBucket(self, pr, bucketId, destroyExistingRemote);
      }
      else if (self.equals(primary)) {
        if (destroyExistingLocal) {
          if (logger.isDebugEnabled()) {
            logger.debug("createPrimaryBucket: {} bucket {} already primary, destroying local primary", pr, bucketId);
          }
          if (dumpBucket(self, region, bucketId)) {
            createdBucket = createBucket(self, region, bucketId, destroyExistingRemote);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("createPrimaryBucket: {} bucket {} already primary, no action needed", pr, bucketId);
          }
        }
      }
      else {
        if (destroyExistingRemote) {
          if (logger.isDebugEnabled()) {
            logger.debug("createPrimaryBucket: {} bucket {},{} is primary, destroying it", pr, bucketId, primary);
          }
          if (dumpBucket(primary, region, bucketId)) {
            createdBucket = createBucket(self, region, bucketId, destroyExistingRemote);
          }
        }
        else {
          Object[] params = new Object[] { self, primary };
          throw new IllegalStateException(
              LocalizedStrings.PartitionManager_BUCKET_CANNOT_BE_MOVED_AS_DESTROYEXISTING_IS_FALSE
              .toLocalizedString(params));
        }
      }
    }
    finally {
      recovLock.unlock();
    }
    return createdBucket;
  }

  /**
   * Get the current primary owner for a bucket. If the bucket is not yet
   * created it returns null immediately
   * 
   * @throws IllegalStateException
   *           if the provided region is something other than a
   *           {@linkplain DataPolicy#PARTITION partitioned Region}
   * @return the primary member for the bucket, possibly null if a primary is
   *         not yet determined
   * @since 6.5
   * @param region
   * @param bucketId
   */
  private static DistributedMember getPrimaryMemberForBucket(
      final Region<?, ?> region, final int bucketId) {
    PartitionedRegion pr = prCheck(region, bucketId);
    if (pr.getRegionAdvisor().isStorageAssignedForBucket(bucketId)) {
      return pr.getBucketPrimary(bucketId);
    }
    else {
      return null;
    }
  }
  
  private static boolean createBucket(final DistributedMember target,
      final Region<?, ?> region, final int bucketId, final boolean destroyExistingRemote) {
    final PartitionedRegion pr = prCheck(region, bucketId);
    InternalDistributedMember createTarget = dataStoreCheck(target, pr);
    
    //This is a bit of a hack. If we don't have a source for a bucket move,
    //createBackupBucketOnMember isn't going to let us exceed redundancy.
    InternalDistributedMember moveSource = createTarget;
    boolean createdBucket = pr.getRedundancyProvider().createBackupBucketOnMember(
        bucketId, createTarget, false, false, moveSource, true);
    
    Set<InternalDistributedMember> currentBucketOwners = pr.getRegionAdvisor()
    .getBucketOwners(bucketId);
    
    boolean includesTarget = currentBucketOwners.remove(target);

    //Race condition. Someone else may have created the bucket while we were
    //trying to create it. If they got in first, we could have exceeded redundancy
    //Based on the flags, we need to get back down to redundancy.
    //Note that nobody else is going to create the bucket now, because
    //redundancy is already satisfied.
    if(currentBucketOwners.size() > pr.getRedundantCopies() && includesTarget) {
      InternalDistributedMember remoteBucket = currentBucketOwners.iterator().next();
      if(destroyExistingRemote) {
        if (logger.isDebugEnabled()) {
          logger.debug("createPrimaryBucket: {}, bucket {} Redundancy is exceeded due to concurrent bucket create, destroying remote bucket: {}", pr, bucketId, remoteBucket);
        }
        dumpBucket(remoteBucket, region, bucketId);
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("createPrimaryBucket: {}, bucket {} Redundancy is exceeded due to concurrent bucket create, destroying my bucket", pr, bucketId);
        }
        dumpBucket(target, region, bucketId);
        Object[] params = new Object[] { target, remoteBucket };
        throw new IllegalStateException(
            LocalizedStrings.PartitionManager_BUCKET_CANNOT_BE_MOVED_AS_DESTROYEXISTING_IS_FALSE
            .toLocalizedString(params));
      }
    }
    return createdBucket;
  }

  private static boolean dumpBucket(final DistributedMember source,
      final Region<?, ?> region, final int bucketId) {
    final PartitionedRegion pr = prCheck(region, bucketId);
    InternalDistributedMember bucketSource = dataStoreCheck(source, pr);
    Set<InternalDistributedMember> currentBucketOwners = pr.getRegionAdvisor()
        .getBucketOwners(bucketId);
    if (!currentBucketOwners.contains(bucketSource)) {
      Object[] params = new Object[] { bucketSource, Integer.valueOf(bucketId) };
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_SOURCE_MEMBER_0_BUCKETID_1_DOES_NOT_HAVE_THE_BUCKET
              .toLocalizedString(params));
    }
    InternalDistributedMember self = (InternalDistributedMember)pr.getCache()
        .getDistributedSystem().getDistributedMember();

    if (bucketSource.equals(self)) {
      PartitionedRegionDataStore dataStore = pr.getDataStore();
      return dataStore.removeBucket(bucketId, true);
    }
    else {
      RemoveBucketResponse response = RemoveBucketMessage.send(bucketSource,
          pr, bucketId, true);
      if (response != null) {
        return response.waitForResponse();
      }
    }
    return false;
  }

  private static PartitionedRegion prCheck(final Region<?, ?> region,
      final int bucketId) {
    if (region == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionRegionHelper_ARGUMENT_REGION_IS_NULL
              .toString());
    }
    if (!(region instanceof PartitionedRegion)) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION
              .toLocalizedString(region.getFullPath()));
    }
    final PartitionedRegion pr = (PartitionedRegion)region;
    if (bucketId < 0 || bucketId >= pr.getTotalNumberOfBuckets()) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_BUCKETID_ARG0_RANGE_0_TO_ARG1_PR_ARG2
              .toLocalizedString(new Object[] { Integer.valueOf(bucketId),
                  Integer.valueOf((pr.getTotalNumberOfBuckets() - 1)),
                  pr.getFullPath() }));
    }
    
    if (pr.isFixedPartitionedRegion()) {
      boolean containsBucket = false;
      List<FixedPartitionAttributesImpl> fpas = pr
          .getFixedPartitionAttributesImpl();
      for (FixedPartitionAttributesImpl fpa : fpas) {
        if (fpa.isPrimary() && (fpa.hasBucket(bucketId))) {
          containsBucket = true;
          break;
        }
      }
      if (!containsBucket)
        throw new IllegalArgumentException(
            LocalizedStrings.FixedPartitionManager_BUCKETID_ARG_PR_ARG2
                .toLocalizedString(new Object[] { Integer.valueOf(bucketId),
                    pr.getFullPath() }));

    }
    return pr;
  }

  private static InternalDistributedMember dataStoreCheck(
      final DistributedMember target, final PartitionedRegion pr) {
    final InternalDistributedMember idmTarget = (InternalDistributedMember)target;
    final boolean localIsDatastore = pr.isDataStore()
        && pr.getCache().getDistributedSystem().getDistributedMember().equals(
            target);
    if (!(localIsDatastore || pr.getRegionAdvisor().adviseDataStore().contains(
        idmTarget))) {
      throw new IllegalArgumentException(
          LocalizedStrings.PartitionManager_PROVIDED_MEMBER_0_NO_PR_OR_NO_DATASTORE
              .toLocalizedString(target));
    }
    return idmTarget;
  }

}
