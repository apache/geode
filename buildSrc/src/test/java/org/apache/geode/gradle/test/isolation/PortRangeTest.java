/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.geode.gradle.test.isolation;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.gradle.testing.isolation.PortRange;

public class PortRangeTest {
  @Test
  public void firstPartitionHasSameLowerBoundAsFullRange() {
    PortRange fullRange = new PortRange(11111, 19994); // Arbitrary
    PortRange firstPartition = fullRange.partition(0, 24);
    assertEquals(fullRange.lowerBound(), firstPartition.lowerBound());
  }

  @Test
  public void lastPartitionHasSameUpperBoundAsSourceRange() {
    PortRange fullRange = new PortRange(3333, 20002); // Arbitrary
    PortRange lastPartition = fullRange.partition(23, 24);
    assertEquals(fullRange.upperBound(), lastPartition.upperBound());
  }

  @Test
  public void adjacentPartitionsHaveAdjacentBounds() {
    PortRange fullRange = new PortRange(22222, 33211); // Arbitrary
    int numberOfPartitions = 24;
    List<PortRange> partitions = IntStream.range(0, numberOfPartitions)
        .mapToObj(i -> fullRange.partition(i, numberOfPartitions))
        .collect(toList());

    for (int i = 1; i < partitions.size(); i++) {
      int lowerPartitionUpperBound = partitions.get(i - 1).upperBound();
      int upperPartitionLowerBound = partitions.get(i).lowerBound();
      boolean areAdjacent = upperPartitionLowerBound == lowerPartitionUpperBound + 1;

      String description = String.format(
          "partition %d upper bound (%d) is adjacent to partition %d lower bound (%d)",
          i - 1, lowerPartitionUpperBound, i, upperPartitionLowerBound);
      assertTrue(description, areAdjacent);
    }
  }

  @Test
  public void partitionSizesDifferByNoMoreThan1() {
    PortRange fullRange = new PortRange(12345, 23451); // Arbitrary
    int numberOfPartitions = 24;

    int[] partitionSizes = IntStream.range(0, numberOfPartitions)
        .mapToObj(i -> fullRange.partition(i, numberOfPartitions))
        .mapToInt(p -> p.upperBound() - p.lowerBound() + 1)
        .sorted()
        .toArray();

    int minPartitionSize = partitionSizes[0];
    int maxPartitionSize = partitionSizes[partitionSizes.length - 1];

    String description = String.format(
        "minimum (%d) and maximum (%d) partition sizes differ by no more than 1",
        minPartitionSize, maxPartitionSize);
    assertTrue(description, maxPartitionSize - minPartitionSize <= 1);
  }
}
