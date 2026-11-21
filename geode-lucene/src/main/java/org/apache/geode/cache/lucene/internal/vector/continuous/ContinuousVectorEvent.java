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
 * Immutable continuous vector notification stamped with Apache Geode Region Version Vector (RVV)
 * metadata {@code (bucketId, memberId, regionVersion)} for idempotent deduplication and monotonic
 * delivery across primary bucket failover.
 */
public final class ContinuousVectorEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String subscriptionId;
  private final Object key;
  private final float similarityScore;
  private final int bucketId;
  private final String memberId;
  private final long regionVersion;
  private final int entryVersion;
  private final long timestamp;
  private final boolean replayed;

  public ContinuousVectorEvent(String subscriptionId, Object key, float similarityScore,
      int bucketId, String memberId, long regionVersion, int entryVersion, long timestamp,
      boolean replayed) {
    if (subscriptionId == null || subscriptionId.isEmpty()) {
      throw new IllegalArgumentException("subscriptionId must not be null or empty");
    }
    if (memberId == null || memberId.isEmpty()) {
      throw new IllegalArgumentException("memberId must not be null or empty");
    }
    if (regionVersion <= 0L) {
      throw new IllegalArgumentException("regionVersion must be > 0, got: " + regionVersion);
    }
    this.subscriptionId = subscriptionId;
    this.key = key;
    this.similarityScore = similarityScore;
    this.bucketId = bucketId;
    this.memberId = memberId;
    this.regionVersion = regionVersion;
    this.entryVersion = entryVersion;
    this.timestamp = timestamp;
    this.replayed = replayed;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public Object getKey() {
    return key;
  }

  public float getSimilarityScore() {
    return similarityScore;
  }

  public int getBucketId() {
    return bucketId;
  }

  public String getMemberId() {
    return memberId;
  }

  public long getRegionVersion() {
    return regionVersion;
  }

  public int getEntryVersion() {
    return entryVersion;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isReplayed() {
    return replayed;
  }

  public RvvStreamKey getStreamKey() {
    return new RvvStreamKey(bucketId, memberId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ContinuousVectorEvent)) {
      return false;
    }
    ContinuousVectorEvent that = (ContinuousVectorEvent) o;
    return Float.compare(that.similarityScore, similarityScore) == 0
        && bucketId == that.bucketId
        && regionVersion == that.regionVersion
        && entryVersion == that.entryVersion
        && timestamp == that.timestamp
        && replayed == that.replayed
        && Objects.equals(subscriptionId, that.subscriptionId)
        && Objects.equals(key, that.key)
        && Objects.equals(memberId, that.memberId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionId, key, similarityScore, bucketId, memberId, regionVersion,
        entryVersion, timestamp, replayed);
  }

  @Override
  public String toString() {
    return "ContinuousVectorEvent{"
        + "subscriptionId='" + subscriptionId + '\''
        + ", key=" + key
        + ", similarityScore=" + similarityScore
        + ", bucketId=" + bucketId
        + ", memberId='" + memberId + '\''
        + ", regionVersion=" + regionVersion
        + ", entryVersion=" + entryVersion
        + ", replayed=" + replayed
        + '}';
  }
}
