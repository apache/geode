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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the unique integer index of a {@link Bucket} as an object for use in
 * {@link Collection} types or other methods requiring objects. If used properly this class avoids
 * the transient allocations of autoboxed {@code int} and {@link Integer}. The default autoboxed
 * {@link Integer} cache is -128 to +127. This class is optimized for positive bucket indexes only.
 */
public final class BucketId implements Comparable<BucketId> {
  static final int max = 256;
  private static final BucketId[] values;

  static {
    values = new BucketId[max];
    for (int i = 0; i < max; i++) {
      values[i] = new BucketId(i);
    }
  }

  public static final BucketId UNKNOWN_BUCKET = new BucketId(-1);

  private final int bucketId;

  private BucketId(final int bucketId) {
    this.bucketId = bucketId;
  }

  /**
   * Returns the {@code int} value for this {@link BucketId}. Be careful not to use the value in
   * any methods that would result in autoboxing.
   *
   * @return {@code int} value for this {@link BucketId}.
   */
  public int intValue() {
    return bucketId;
  }

  /**
   * Returns the {@link BucketId} for given {@code int} value.
   *
   * @param bucketId {@code int} value to get {@link BucketId} for.
   * @return BucketId or throws {@link IndexOutOfBoundsException}.
   * @throws IndexOutOfBoundsException if bucketId is out of range.
   */
  public static BucketId valueOf(final int bucketId) {
    if (-1 == bucketId) {
      return UNKNOWN_BUCKET;
    }
    return values[bucketId];
  }

  public static @Nullable Set<BucketId> fromIntValues(final int @Nullable [] intValues) {
    if (null == intValues) {
      return null;
    }

    final Set<BucketId> bucketIds = new HashSet<>(intValues.length);
    for (final int intValue : intValues) {
      bucketIds.add(BucketId.valueOf(intValue));
    }
    return bucketIds;
  }

  public static int @Nullable [] toIntValues(@Nullable Collection<BucketId> bucketIds) {
    if (null == bucketIds) {
      return null;
    }

    final int[] intValues = new int[bucketIds.size()];
    int i = 0;
    for (final BucketId bucketId : bucketIds) {
      intValues[i++] = bucketId.intValue();
    }
    return intValues;
  }

  @Override
  public boolean equals(final Object object) {
    if (object instanceof BucketId) {
      return bucketId == ((BucketId) object).bucketId;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(bucketId);
  }

  @Override
  public String toString() {
    return Integer.toString(bucketId);
  }

  @Override
  public int compareTo(@NotNull final BucketId other) {
    return Integer.compare(bucketId, other.bucketId);
  }
}
