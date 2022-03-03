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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Internal implementation of {@link FixedPartitionAttributes}.
 *
 * @since GemFire 6.6
 */

public class FixedPartitionAttributesImpl extends FixedPartitionAttributes
    implements DataSerializable {
  private static final long serialVersionUID = 7435010874879693776L;

  /**
   * name of the partition
   */
  private String partitionName;

  /**
   * represents primary status
   */
  private boolean isPrimary = false;

  /**
   * number of buckets allowed to create for this partition.
   */
  private int numBuckets = 1;

  private int startingBucketID = KeyInfo.UNKNOWN_BUCKET;

  /**
   * Constructs an instance of <code>FixedPartitionAttributes</code> with default settings.
   */
  public FixedPartitionAttributesImpl() {

  }

  @Override
  public String getPartitionName() {
    return partitionName;
  }

  @Override
  public int getNumBuckets() {
    return numBuckets;
  }

  @Override
  public boolean isPrimary() {
    return isPrimary;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    partitionName = DataSerializer.readString(in);
    isPrimary = in.readBoolean();
    numBuckets = in.readInt();
    startingBucketID = in.readInt();

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(partitionName, out);
    out.writeBoolean(isPrimary);
    out.writeInt(numBuckets);
    out.writeInt(startingBucketID);
  }

  public FixedPartitionAttributesImpl setPartitionName(String name) {
    partitionName = name;
    return this;
  }

  public FixedPartitionAttributesImpl isPrimary(boolean isPrimary2) {
    isPrimary = isPrimary2;
    return this;
  }

  public FixedPartitionAttributesImpl setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
    return this;
  }

  public void setStartingBucketID(int startingBucketID) {
    this.startingBucketID = startingBucketID;
  }

  public int getStartingBucketID() {
    return startingBucketID;
  }

  public int getLastBucketID() {
    return startingBucketID + numBuckets - 1;
  }

  public boolean hasBucket(int bucketId) {
    return getStartingBucketID() <= bucketId && bucketId <= getLastBucketID();
  }

  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FixedPartitionAttributesImpl)) {
      return false;
    }
    FixedPartitionAttributesImpl spr = (FixedPartitionAttributesImpl) obj;
    return spr.getPartitionName().equals(getPartitionName());
  }

  public int hashCode() {
    return getPartitionName().hashCode();
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("FixedPartitionAttributes@").append("[partitionName=").append(partitionName)
        .append(";isPrimary=").append(isPrimary).append(";numBuckets=")
        .append(numBuckets);
    if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "PRDebug")) {
      s.append(";startingBucketID= ").append(startingBucketID);
    }
    s.append("]");
    return s.toString();
  }
}
