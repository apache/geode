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

package org.apache.geode.cache.partition;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.partitioned.BucketId;

/**
 *
 * A callback for partitioned regions, invoked when a partition region is created or any bucket is
 * created/deleted or any bucket in a partitioned region becomes primary.<br>
 * <br>
 * It is highly recommended that implementations of this listener should be quick and not try to
 * manipulate regions and data because the callbacks are invoked while holding locks that may
 * block region operations. <br>
 *
 * <pre>
 * package com.myCompany.MyPartitionListener;
 *
 * public class MyPartitionListener extends PartitionListenerAdapter implements Declarable {
 *   private String regionName;
 *
 *   public MyPartitionListener() {}
 *
 *   public void afterPrimary(BucketId bucketId) {
 *     System.out.println("bucket:" + bucketId + " has become primary on " + this.regionName);
 *   }
 *
 *   public void afterRegionCreate(Region&lt;?, ?&gt; region) {
 *     this.regionName = region.getName();
 *   }
 * }
 * </pre>
 *
 * A sample declaration of the MyPartitionListener in cache.xml as follows :<br>
 *
 * <pre>
 * &lt;partition-attributes redundant-copies=&quot;1&quot;&gt;
 *     &lt;partition-listener&gt;
 *         &lt;class-name&gt;com.myCompany.MyPartitionListener&lt;/class-name&gt;
 *     &lt;/partition-listener&gt;
 * &lt;/partition-attributes&gt;
 * </pre>
 *
 * @see PartitionAttributesFactory#addPartitionListener(PartitionListener)
 *
 * @since GemFire 6.5
 *
 */
public interface PartitionListener {

  /**
   * Callback invoked when any bucket in a partitioned region becomes primary
   *
   * @param bucketId id of the bucket which became primary
   * @since GemFire 6.5
   * @deprecated Use {@link #afterPrimary(BucketId)}
   */
  @Deprecated
  void afterPrimary(int bucketId);

  /**
   * Callback invoked when any bucket in a partitioned region becomes primary
   *
   * @param bucketId id of the bucket which became primary
   */
  default void afterPrimary(BucketId bucketId) {
    afterPrimary(bucketId.intValue());
  }

  /**
   * Callback invoked when any bucket in a partitioned region stops being primary
   *
   * @param bucketId id of the bucket which stopped being primary
   * @since Geode 1.1
   * @deprecated Use {@link #afterSecondary(BucketId)}
   */
  @Deprecated
  default void afterSecondary(int bucketId) {
  }

  /**
   * Callback invoked when any bucket in a partitioned region stops being primary
   *
   * @param bucketId id of the bucket which stopped being primary
   */
  default void afterSecondary(BucketId bucketId) {
    afterSecondary(bucketId.intValue());
  }

  /**
   * Callback invoked when a partition region is created
   *
   * @param region handle of the region which is created
   * @since GemFire 6.5
   */
  void afterRegionCreate(Region<?, ?> region);

  /**
   * Callback invoked after a bucket has been removed from a member (e.g. during rebalancing). This
   * API is useful for maintaining external data structures by bucket id or key.
   *
   * @param bucketId id of the bucket removed
   * @param keys keys in the bucket removed
   * @since GemFire 6.6.1
   * @deprecated Use {@link #afterBucketRemoved(BucketId, Iterable)}
   */
  @Deprecated
  void afterBucketRemoved(int bucketId, Iterable<?> keys);

  /**
   * Callback invoked after a bucket has been removed from a member (e.g. during rebalancing). This
   * API is useful for maintaining external data structures by bucket id or key.
   *
   * @param bucketId id of the bucket removed
   * @param keys keys in the bucket removed
   */
  default void afterBucketRemoved(BucketId bucketId, Iterable<?> keys) {
    afterBucketRemoved(bucketId.intValue(), keys);
  }

  /**
   * Callback invoked after a bucket has been created in a member (e.g. during rebalancing). This
   * API is useful for maintaining external data structures by bucket id or key. Note that this API
   * is invoked after the initial image has been completed so creates and destroys may occur in the
   * keys. It is best to use this API during periods of no cache activity.
   *
   * @param bucketId id of the bucket created
   * @param keys keys in the bucket created
   * @since GemFire 6.6.1
   * @deprecated Use {@link #afterBucketCreated(BucketId, Iterable)}
   */
  @Deprecated
  void afterBucketCreated(int bucketId, Iterable<?> keys);

  /**
   * Callback invoked after a bucket has been created in a member (e.g. during rebalancing). This
   * API is useful for maintaining external data structures by bucket id or key. Note that this API
   * is invoked after the initial image has been completed so creates and destroys may occur in the
   * keys. It is best to use this API during periods of no cache activity.
   *
   * @param bucketId id of the bucket created
   * @param keys keys in the bucket created
   */
  default void afterBucketCreated(BucketId bucketId, Iterable<?> keys) {
    afterBucketCreated(bucketId.intValue(), keys);
  }

}
