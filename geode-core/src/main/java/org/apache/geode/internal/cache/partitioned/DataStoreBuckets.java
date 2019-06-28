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

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class DataStoreBuckets {

  private final InternalDistributedMember memberId;
  private final int numBuckets;
  private final int numPrimaries;
  private final int localMaxMemoryMB;

  public DataStoreBuckets(InternalDistributedMember mem, int buckets, int primaryBuckets,
      int localMaxMemory) {
    memberId = mem;
    numBuckets = buckets;
    numPrimaries = primaryBuckets;
    localMaxMemoryMB = localMaxMemory;
  }

  public InternalDistributedMember memberId() {
    return memberId;
  }

  public int numBuckets() {
    return numBuckets;
  }

  public int numPrimaries() {
    return numPrimaries;
  }

  public int localMaxMemoryMB() {
    return localMaxMemoryMB;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DataStoreBuckets)) {
      return false;
    }
    DataStoreBuckets other = (DataStoreBuckets) obj;
    return numBuckets == other.numBuckets && memberId.equals(other.memberId);
  }

  @Override
  public int hashCode() {
    return memberId.hashCode();
  }

  @Override
  public String toString() {
    return "DataStoreBuckets memberId=" + memberId + "; numBuckets=" + numBuckets
        + "; numPrimaries=" + numPrimaries;
  }
}
