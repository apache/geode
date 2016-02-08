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
