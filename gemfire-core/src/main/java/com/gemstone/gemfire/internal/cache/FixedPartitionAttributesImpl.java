/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;

/**
 * Internal implementation of {@link FixedPartitionAttributes}.
 * 
 * @author kbachhav
 * @since 6.6
 */

public class FixedPartitionAttributesImpl extends FixedPartitionAttributes
    implements DataSerializable{
  /**
   * 
   */
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
   * Constructs an instance of <code>FixedPartitionAttributes</code> with
   * default settings.
   */
  public FixedPartitionAttributesImpl() {

  }

  public String getPartitionName() {
    return this.partitionName;
  }

  public int getNumBuckets() {
    return this.numBuckets;
  }

  public boolean isPrimary() {
    return this.isPrimary;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.partitionName = DataSerializer.readString(in);
    this.isPrimary = in.readBoolean();
    this.numBuckets = in.readInt();
    this.startingBucketID = in.readInt();

  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.partitionName, out);
    out.writeBoolean(this.isPrimary);
    out.writeInt(this.numBuckets);
    out.writeInt(this.startingBucketID);
  }

  public FixedPartitionAttributesImpl setPartitionName(String name) {
    this.partitionName = name;
    return this;
  }

  public FixedPartitionAttributesImpl isPrimary(boolean isPrimary2) {
    this.isPrimary = isPrimary2;
    return this;
  }

  public FixedPartitionAttributesImpl setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
    return this;
  }
  public void setStartingBucketID(int startingBucketID) {
    this.startingBucketID  = startingBucketID;
  }
  
  public int getStartingBucketID() {
    return startingBucketID;
  }
  
  public int getLastBucketID() {
    return startingBucketID + numBuckets -1;
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
    FixedPartitionAttributesImpl spr = (FixedPartitionAttributesImpl)obj;
    if (spr.getPartitionName().equals(this.getPartitionName())) {
      return true;
    }
    return false;
  }

  public int hashCode() {
    return this.getPartitionName().hashCode();
  }
  
  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append("FixedPartitionAttributes@").append("[partitionName=").append(
        this.partitionName).append(";isPrimary=").append(this.isPrimary)
        .append(";numBuckets=").append(this.numBuckets);
    if (Boolean.getBoolean("gemfire.PRDebug")) {
      s.append(";startingBucketID= ").append(this.startingBucketID);
    }
    s.append("]");
    return s.toString();
  }
}
