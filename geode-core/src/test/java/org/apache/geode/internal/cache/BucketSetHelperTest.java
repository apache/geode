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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Test;


public class BucketSetHelperTest {

  @Test
  public void testAddGetLength() {
    final int[] buckets = new int[8];
    assertThat(BucketSetHelper.length(buckets)).isEqualTo(0);
    BucketSetHelper.add(buckets, 1);
    BucketSetHelper.add(buckets, 2);
    BucketSetHelper.add(buckets, 3);
    assertThat(BucketSetHelper.length(buckets)).isEqualTo(3);
    assertThat(BucketSetHelper.get(buckets, 0)).isEqualTo(1);
    assertThat(BucketSetHelper.get(buckets, 1)).isEqualTo(2);
    assertThat(BucketSetHelper.get(buckets, 2)).isEqualTo(3);
  }

  public void testAllFunctions() {
    final int[] buckets = new int[8];
    assertThat(BucketSetHelper.length(buckets)).isEqualTo(0);
    for (int i = 0; i < 7; i++) {
      BucketSetHelper.add(buckets, i);
    }
    assertThat(BucketSetHelper.length(buckets)).isEqualTo(7);
    assertThat(BucketSetHelper.get(buckets, 0)).isEqualTo(0);
    assertThat(BucketSetHelper.get(buckets, 3)).isEqualTo(3);
    assertThat(BucketSetHelper.get(buckets, 5)).isEqualTo(5);

    Set<Integer> testSet = BucketSetHelper.toSet(buckets);

    assertThat(testSet.size()).isEqualTo(7);

    int[] testArray = BucketSetHelper.fromSet(testSet);

    assertThat(buckets).isEqualTo(testArray);

  }
}
