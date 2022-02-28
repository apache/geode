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

import static org.apache.geode.internal.cache.partitioned.BucketId.nonNull;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class BucketIdTest {

  @Test
  public void returnsUnknownBucketWhenOutOfBounds() {
    assertThat(BucketId.valueOf(-2)).isSameAs(BucketId.UNKNOWN_BUCKET);
    assertThat(BucketId.valueOf(BucketId.MAX + 1)).isSameAs(BucketId.UNKNOWN_BUCKET);
  }

  @Test
  public void returnsSameValueForMultipleCalls() {
    for (int i = 0; i < BucketId.MAX; i++) {
      assertThat(BucketId.valueOf(i)).isSameAs(BucketId.valueOf(i));
    }
  }

  @Test
  public void returnsValueForIntValue() {
    for (int i = 0; i < BucketId.MAX; i++) {
      assertThat(BucketId.valueOf(i).intValue()).isEqualTo(i);
    }
  }

  @Test
  public void returnsUnknownBucketForNegativeOne() {
    assertThat(BucketId.valueOf(-1)).isSameAs(BucketId.UNKNOWN_BUCKET);
  }

  @Test
  public void nonNullReturnsOriginalBucketIdWhenNotNull() {
    final BucketId bucketId = BucketId.valueOf(2);
    assertThat(nonNull(bucketId)).isSameAs(bucketId);
  }

  @Test
  public void nonNullReturnsUnknownBucketWhenUnknownBucket() {
    assertThat(nonNull(BucketId.UNKNOWN_BUCKET)).isSameAs(BucketId.UNKNOWN_BUCKET);
  }

  @Test
  public void nonNullReturnsUnknownBucketWhenNull() {
    assertThat(nonNull(null)).isSameAs(BucketId.UNKNOWN_BUCKET);
  }
}
