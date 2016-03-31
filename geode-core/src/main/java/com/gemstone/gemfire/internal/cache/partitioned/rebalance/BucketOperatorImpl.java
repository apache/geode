package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Map;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionRebalanceOp;

public class BucketOperatorImpl implements BucketOperator {
  
  private PartitionedRegionRebalanceOp rebalanceOp;

  public BucketOperatorImpl(PartitionedRegionRebalanceOp rebalanceOp) {
    this.rebalanceOp = rebalanceOp;
  }

  @Override
  public boolean moveBucket(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId,
      Map<String, Long> colocatedRegionBytes) {

    InternalResourceManager.getResourceObserver().movingBucket(
        rebalanceOp.getLeaderRegion(), bucketId, source, target);
    return rebalanceOp.moveBucketForRegion(source, target, bucketId);
  }

  @Override
  public boolean movePrimary(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId) {

    InternalResourceManager.getResourceObserver().movingPrimary(
        rebalanceOp.getLeaderRegion(), bucketId, source, target);
    return rebalanceOp.movePrimaryBucketForRegion(target, bucketId); 
  }

  @Override
  public void createRedundantBucket(
      InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes, Completion completion) {
    boolean result = false;
    try {
      result = rebalanceOp.createRedundantBucketForRegion(targetMember, bucketId);
    } finally {
      if(result) {
        completion.onSuccess();
      } else {
        completion.onFailure();
      }
    }
  }
  
  @Override
  public void waitForOperations() {
    //do nothing, all operations are synchronous
  }

  @Override
  public boolean removeBucket(InternalDistributedMember targetMember, int bucketId,
      Map<String, Long> colocatedRegionBytes) {
    return rebalanceOp.removeRedundantBucketForRegion(targetMember, bucketId);
  }
}