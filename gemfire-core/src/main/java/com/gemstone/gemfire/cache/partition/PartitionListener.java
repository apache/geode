/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;

/**
 * 
 * A callback for partitioned regions, invoked when a partition region is
 * created or any bucket in a partitioned region becomes primary.<br>
 * <br>
 * A sample implementation of this interface to colocate partition regions using
 * a primary key without having to honor the redundancy contract for every
 * colocate partition regions is as follows : <br>
 * 
 * <pre>
 * public class ColocatingPartitionListener extends PartitionListenerAdapter
 *     implements Declarable {
 *   private Cache cache;
 * 
 *   private List&lt;String&gt; viewRegionNames = new ArrayList&lt;String&gt;();
 * 
 *   public ColocatingPartitionListener() {
 *   }
 * 
 *   public void afterPrimary(int bucketId) {
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
 * A sample declaration of the ColocatingPartitionListener in cache.xml as
 * follows :<br>
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
 * @see PartitionAttributesFactory#addPartitionListener(PartitionListener)
 * 
 *      Note : Please contact support@gemstone.com before using these APIs
 * 
 * @author Yogesh Mahajan
 * @since 6.5
 * 
 */
public interface PartitionListener {

  /**
   * Callback invoked when any bucket in a partitioned region becomes primary
   * 
   * @param bucketId
   *          id of the bucket which became primary
   * @since 6.5
   */
  public void afterPrimary(int bucketId);

  /**
   * Callback invoked when a partition region is created
   * 
   * @param region
   *          handle of the region which is created
   * @since 6.5
   */
  public void afterRegionCreate(Region<?, ?> region);

  /**
   * Callback invoked after a bucket has been removed from a member (e.g. during
   * rebalancing). This API is useful for maintaining external data structures
   * by bucket id or key.
   * 
   * @param bucketId
   *          id of the bucket removed
   * @param keys
   *          keys in the bucket removed
   * @since 6.6.1
   */
  public void afterBucketRemoved(int bucketId, Iterable<?> keys);

  /**
   * Callback invoked after a bucket has been created in a member (e.g. during
   * rebalancing). This API is useful for maintaining external data structures
   * by bucket id or key. Note that this API is invoked after the initial image
   * has been completed so creates and destroys may occur in the keys. It is
   * best to use this API during periods of no cache activity.
   * 
   * @param bucketId
   *          id of the bucket created
   * @param keys
   *          keys in the bucket created
   * @since 6.6.1
   */
  public void afterBucketCreated(int bucketId, Iterable<?> keys);
}
