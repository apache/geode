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
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.Version;

/**
 * Arguments passed to the HDFSForceCompactionFunction
 * 
 * @author sbawaska
 */
@SuppressWarnings("serial")
public class HDFSForceCompactionArgs implements VersionedDataSerializable {

  private static Version[] serializationVersions = new Version[]{ Version.GFE_81 };

  private HashSet<Integer> buckets;

  private boolean isMajor;

  private int maxWaitTime;

  public HDFSForceCompactionArgs() {
  }

  public HDFSForceCompactionArgs(Set<Integer> buckets, boolean isMajor, Integer maxWaitTime) {
    this.buckets = new HashSet<Integer>(buckets);
    this.isMajor = isMajor;
    this.maxWaitTime = maxWaitTime;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet(buckets, out);
    out.writeBoolean(isMajor);
    out.writeInt(maxWaitTime);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.buckets = DataSerializer.readHashSet(in);
    this.isMajor = in.readBoolean();
    this.maxWaitTime = in.readInt();
  }

  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  public Set<Integer> getBuckets() {
    return (Set<Integer>) buckets;
  }

  public void setBuckets(Set<Integer> buckets) {
    this.buckets = new HashSet<Integer>(buckets);
  }

  public boolean isMajor() {
    return isMajor;
  }

  public void setMajor(boolean isMajor) {
    this.isMajor = isMajor;
  }

  public boolean isSynchronous() {
    return maxWaitTime == 0;
  }

  public int getMaxWaitTime() {
    return this.maxWaitTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@")
    .append(System.identityHashCode(this))
    .append(" buckets:").append(buckets)
    .append(" isMajor:").append(isMajor)
    .append(" maxWaitTime:").append(maxWaitTime);
    return sb.toString();
  }
}
