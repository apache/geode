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

package org.apache.geode.cache.lucene.internal.vector.continuous;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable identifier for a Region Version Vector (RVV) stream coordinate {@code (bucketId,
 * memberId)}.
 */
public final class RvvStreamKey implements Serializable, Comparable<RvvStreamKey> {
  private static final long serialVersionUID = 1L;

  private final int bucketId;
  private final String memberId;

  public RvvStreamKey(int bucketId, String memberId) {
    if (memberId == null || memberId.isEmpty()) {
      throw new IllegalArgumentException("memberId must not be null or empty");
    }
    this.bucketId = bucketId;
    this.memberId = memberId;
  }

  public int getBucketId() {
    return bucketId;
  }

  public String getMemberId() {
    return memberId;
  }

  @Override
  public int compareTo(RvvStreamKey other) {
    int cmp = Integer.compare(this.bucketId, other.bucketId);
    if (cmp != 0) {
      return cmp;
    }
    return this.memberId.compareTo(other.memberId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RvvStreamKey)) {
      return false;
    }
    RvvStreamKey that = (RvvStreamKey) o;
    return bucketId == that.bucketId && Objects.equals(memberId, that.memberId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketId, memberId);
  }

  @Override
  public String toString() {
    return "RvvStreamKey{bucketId=" + bucketId + ", memberId='" + memberId + "'}";
  }
}
