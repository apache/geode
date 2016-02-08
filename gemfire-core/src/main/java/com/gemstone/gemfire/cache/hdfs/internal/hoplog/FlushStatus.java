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

import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.Version;

/**
 * Reports the result of a flush request.
 * 
 * @author bakera
 */
public class FlushStatus implements VersionedDataSerializable {
  private static Version[] serializationVersions = new Version[]{ Version.GFE_81 };
  private int bucketId;

  private final static int LAST = -1;
  
  public FlushStatus() {
  }

  public static FlushStatus last() {
    return new FlushStatus(LAST);
  }
  
  public FlushStatus(int bucketId) {
    this.bucketId = bucketId;
  }
  public int getBucketId() {
    return bucketId;
  }
  public boolean isLast() {
    return bucketId == LAST;
  }
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(bucketId);
  }
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.bucketId = in.readInt();
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
    .append(bucketId);
    return sb.toString();
  }
}
