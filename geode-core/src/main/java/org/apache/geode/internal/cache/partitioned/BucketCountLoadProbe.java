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
package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A load probe which calculates the load of a pr using the just the number of buckets on a member.
 *
 */
public class BucketCountLoadProbe implements LoadProbe, DataSerializableFixedID {
  private static final long serialVersionUID = 7040814060882774875L;

  @Override
  public PRLoad getLoad(PartitionedRegion pr) {
    PartitionedRegionDataStore ds = pr.getDataStore();
    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    PRLoad prLoad = new PRLoad(configuredBucketCount, pr.getLocalMaxMemory());

    // key: bid, value: size
    for (Integer bidInt : ds.getAllLocalBucketIds()) {
      int bid = bidInt;

      BucketAdvisor bucketAdvisor = pr.getRegionAdvisor().getBucket(bid).getBucketAdvisor();
      // Wait for a primary to exist for this bucket, because
      // it might be this member.
      bucketAdvisor.getPrimary();
      boolean isPrimary = pr.getRegionAdvisor().getBucket(bid).getBucketAdvisor().isPrimary();
      prLoad.addBucket(bid, 1, isPrimary ? 1 : 0);
    }

    return prLoad;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {}

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {}

  @Override
  public int getDSFID() {
    return BUCKET_COUNT_LOAD_PROBE;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

}
