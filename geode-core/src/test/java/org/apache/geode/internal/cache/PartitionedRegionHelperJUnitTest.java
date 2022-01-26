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

import org.junit.Test;

import org.apache.geode.cache.Region;


public class PartitionedRegionHelperJUnitTest {

  @Test
  public void testEscapeUnescape() {
    String bucketName =
        PartitionedRegionHelper
            .getBucketName(Region.SEPARATOR + "root" + Region.SEPARATOR + "region", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(Region.SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(Region.SEPARATOR + "root" + Region.SEPARATOR + "region");

    bucketName =
        PartitionedRegionHelper
            .getBucketName(Region.SEPARATOR + "root" + Region.SEPARATOR + "region_one", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(Region.SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(Region.SEPARATOR + "root" + Region.SEPARATOR + "region_one");
  }

  @Test
  public void testEscapeUnescapeWhenRegionHasUnderscoreInTheName() {
    String bucketName =
        PartitionedRegionHelper.getBucketName(Region.SEPARATOR + "_region", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(Region.SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(Region.SEPARATOR + "_region");

    bucketName =
        PartitionedRegionHelper
            .getBucketName(Region.SEPARATOR + "root" + Region.SEPARATOR + "_region_one", 5);
    assertThat(bucketName).as("Name = " + bucketName).doesNotContain(Region.SEPARATOR);
    assertThat(PartitionedRegionHelper.getPRPath(bucketName))
        .isEqualTo(Region.SEPARATOR + "root" + Region.SEPARATOR + "_region_one");
  }
}
