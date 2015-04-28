/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A load probe which calculates the load of a pr using
 * the size of the buckets in bytes.
 * 
 * @author dsmith
 * @since 6.0
 */
public class SizedBasedLoadProbe implements LoadProbe, DataSerializableFixedID {
  private static final long serialVersionUID = 7040814060882774875L;
  //TODO rebalancing come up with a better threshold for minumum bucket size?
  public static final int MIN_BUCKET_SIZE = Integer.getInteger("gemfire.MIN_BUCKET_SIZE", 1).intValue();

  public PRLoad getLoad(PartitionedRegion pr) {
    PartitionedRegionDataStore ds = pr.getDataStore();
    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    PRLoad prLoad = new PRLoad(
        configuredBucketCount, pr.getLocalMaxMemory());
    
    // key: bid, value: size
    for(Integer bidInt : ds.getAllLocalBucketIds()) {
      int bid = bidInt.intValue();
      long bucketSize = ds.getBucketSize(bid);
      if(bucketSize < MIN_BUCKET_SIZE) {
        bucketSize = MIN_BUCKET_SIZE;
      }
      
      BucketAdvisor bucketAdvisor = pr.getRegionAdvisor().
      getBucket(bid).getBucketAdvisor();
      //Wait for a primary to exist for this bucket, because
      //it might be this member.
      bucketAdvisor.getPrimary();
      boolean isPrimary = pr.getRegionAdvisor().
          getBucket(bid).getBucketAdvisor().isPrimary();
      prLoad.addBucket(bid, bucketSize, isPrimary ? 1 : 0);
    }
    
    return prLoad;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

  public void toData(DataOutput out) throws IOException {
  }

  public int getDSFID() {
    return SIZED_BASED_LOAD_PROBE;
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

}
