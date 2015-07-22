/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Map;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A BucketOperator is used by the PartitionedRegionLoadModel to perform the actual
 * operations such as moving a bucket or creating a redundant copy.
 * @author dsmith
 *
 */
public interface BucketOperator {

  /**
   * Create a redundancy copy of a bucket on a given node. This call may be
   * asynchronous, it will notify the completion when the the operation is done.
   * 
   * Note that the completion is not required to be threadsafe, so implementors
   * should ensure the completion is invoked by the calling thread of
   * createRedundantBucket, usually by invoking the completions in waitForOperations.
   * 
   * @param targetMember
   *          the node to create the bucket on
   * @param bucketId
   *          the id of the bucket to create
   * @param colocatedRegionBytes
   *          the size of the bucket in bytes
   * @param completion
   *          a callback which will receive a notification on the success or
   *          failure of the operation.
   */
  void createRedundantBucket(InternalDistributedMember targetMember,
      int bucketId, Map<String, Long> colocatedRegionBytes, Completion completion);

  /**
   * Remove a bucket from the target member.
   */
  boolean removeBucket(InternalDistributedMember memberId, int id,
      Map<String, Long> colocatedRegionSizes);

  /**
   * Move a bucket from one member to another
   * @param sourceMember The member we want to move the bucket off of. 
   * @param targetMember The member we want to move the bucket too.
   * @param bucketId the id of the bucket we want to move
   * @return true if the bucket was moved successfully
   */
  boolean moveBucket(InternalDistributedMember sourceMember,
      InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes);

  /**
   * Move a primary from one node to another. This method will
   * not be called unless both nodes are hosting the bucket, and the source
   * node is the primary for the bucket.
   * @param source The old primary for the bucket
   * @param target The new primary for the bucket
   * @param bucketId The id of the bucket to move;
   * @return true if the primary was successfully moved.
   */
  boolean movePrimary(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId);
  
  /**
   * Wait for any pending asynchronous operations that this thread submitted
   * earlier to complete. Currently only createRedundantBucket may be
   * asynchronous.
   */
  public void waitForOperations();
  
  /**
   * Callbacks for asnychonous operations. These methods will be invoked when an
   * ansynchronous operation finishes.
   * 
   * The completions are NOT THREADSAFE.
   * 
   * They will be completed when createRedundantBucket or waitForOperations is
   * called.
   */
  public interface Completion {
    public void onSuccess();
    public void onFailure();
  }
}