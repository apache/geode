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
 * @since GemFire 6.0
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
