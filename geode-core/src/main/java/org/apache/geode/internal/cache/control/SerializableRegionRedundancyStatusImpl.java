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
package org.apache.geode.internal.cache.control;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.management.internal.operation.RegionRedundancyStatusImpl;

/**
 * result object produced by the servers. These need to be transferred to the locators
 * via functions so they need to be DataSerializable
 */
public class SerializableRegionRedundancyStatusImpl extends
    RegionRedundancyStatusImpl
    implements DataSerializableFixedID {
  /**
   * Default constructor used for serialization
   */
  public SerializableRegionRedundancyStatusImpl() {
    status = RedundancyStatus.NOT_SATISFIED;
  }

  public SerializableRegionRedundancyStatusImpl(PartitionedRegion region) {
    regionName = region.getName();
    configuredRedundancy = region.getRedundantCopies();
    actualRedundancy = calculateLowestRedundancy(region);
    status = determineStatus(configuredRedundancy, actualRedundancy);
  }

  /**
   * Calculates the lowest redundancy for any bucket in the region.
   *
   * @param region The region for which the lowest redundancy should be calculated.
   * @return The redundancy of the least redundant bucket in the region.
   */
  int calculateLowestRedundancy(PartitionedRegion region) {
    int numBuckets = region.getPartitionAttributes().getTotalNumBuckets();
    int minRedundancy = Integer.MAX_VALUE;
    for (int i = 0; i < numBuckets; i++) {
      int bucketRedundancy = region.getRegionAdvisor().getBucketRedundancy(i);
      // Only consider redundancy for buckets that exist. Buckets that have not been created yet
      // have a redundancy value of -1
      if (bucketRedundancy != -1 && bucketRedundancy < minRedundancy) {
        minRedundancy = bucketRedundancy;
      }
    }
    return minRedundancy == Integer.MAX_VALUE ? 0 : minRedundancy;
  }

  /**
   * Determines the {@link RedundancyStatus} for the region. If redundancy is not configured (i.e.
   * configured redundancy = 0), this always returns {@link RedundancyStatus#SATISFIED}.
   *
   * @param desiredRedundancy The configured redundancy of the region.
   * @param actualRedundancy The actual redundancy of the region.
   * @return The {@link RedundancyStatus} for the region.
   */
  private RedundancyStatus determineStatus(int desiredRedundancy, int actualRedundancy) {
    boolean zeroRedundancy = desiredRedundancy == 0;
    if (actualRedundancy == 0) {
      return zeroRedundancy ? RedundancyStatus.SATISFIED : RedundancyStatus.NO_REDUNDANT_COPIES;
    }
    return desiredRedundancy == actualRedundancy ? RedundancyStatus.SATISFIED
        : RedundancyStatus.NOT_SATISFIED;
  }

  @Override
  public String toString() {
    return String.format(OUTPUT_STRING, regionName, status.name(), configuredRedundancy,
        actualRedundancy);
  }

  @Override
  public int getDSFID() {
    return REGION_REDUNDANCY_STATUS;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    DataSerializer.writeString(regionName, out);
    DataSerializer.writeEnum(status, out);
    out.writeInt(configuredRedundancy);
    out.writeInt(actualRedundancy);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    this.regionName = DataSerializer.readString(in);
    this.status = DataSerializer.readEnum(RedundancyStatus.class, in);
    this.configuredRedundancy = in.readInt();
    this.actualRedundancy = in.readInt();
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
