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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
<<<<<<< HEAD
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
=======
>>>>>>> changes after review

import org.junit.Test;

import org.apache.geode.cache.Cache;


public class PartitionedRegionHelperJUnitTest {

  @Test
  public void testEscapeUnescape() {
    String bucketName =
        PartitionedRegionHelper.getBucketName(SEPARATOR + "root" + SEPARATOR + "region", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(SEPARATOR + "root" + SEPARATOR + "region");

    bucketName =
        PartitionedRegionHelper.getBucketName(SEPARATOR + "root" + SEPARATOR + "region_one", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(SEPARATOR + "root" + SEPARATOR + "region_one");
  }

  @Test
  public void testGetPartitionedRegionUsingBucketRegionName() {
    Cache cache = mock(Cache.class);
    String fullPath = "__PR/_B__partitionedRegion_66";

    // cache == null
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(null, fullPath))
        .isNull();

    // fullPath == null
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(cache, null))
        .isNull();

    // fullPath == ""
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(cache, ""))
        .isNull();

    // fullPath == arbitrary string
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(cache, "abcdef"))
        .isNull();

    // fullPath represents an InternalRegion
    InternalRegion internalRegion = mock(InternalRegion.class);
    when(cache.getRegion("partitionedRegion")).thenReturn(internalRegion);
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(cache, fullPath))
        .isNull();

    // fullPath represents a PartitionedRegion
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(cache.getRegion("/partitionedRegion")).thenReturn(partitionedRegion);
    assertThat(PartitionedRegionHelper.getPartitionedRegionUsingBucketRegionName(cache, fullPath))
        .isEqualTo(partitionedRegion);
  }

  public void testEscapeUnescapeWhenRegionHasUnderscoreInTheName() {
    String bucketName =
        PartitionedRegionHelper.getBucketName(SEPARATOR + "_region", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(SEPARATOR + "_region");

    bucketName =
        PartitionedRegionHelper.getBucketName(SEPARATOR + "root" + SEPARATOR + "_region_one", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(SEPARATOR + "root" + SEPARATOR + "_region_one");
  }
}
