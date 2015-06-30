/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.Version;

/**
 * Status of the compaction task reported in the future
 * 
 * @author sbawaska
 */
public class CompactionStatus implements VersionedDataSerializable {
  /**MergeGemXDHDFSToGFE check and verify serializationversions **/
 
  private static Version[] serializationVersions = new Version[]{ Version.GFE_81 };
  private int bucketId;
  private boolean status;

  public CompactionStatus() {
  }

  public CompactionStatus(int bucketId, boolean status) {
    this.bucketId = bucketId;
    this.status = status;
  }
  public int getBucketId() {
    return bucketId;
  }
  public boolean isStatus() {
    return status;
  }
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(bucketId);
    out.writeBoolean(status);
  }
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.bucketId = in.readInt();
    this.status = in.readBoolean();
  }
  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@")
    .append(System.identityHashCode(this)).append(" Bucket:")
    .append(bucketId).append(" status:").append(status);
    return sb.toString();
  }
}
