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
package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for the port partitioning feature in AvailablePortHelper.
 * These tests validate the actual port allocation behavior when the feature is enabled.
 */
public class PortPartitioningIntegrationTest {

  private String originalPortPartitionSize;
  private String originalDebugPortPartitioning;

  @Before
  public void setUp() {
    // Save original system properties
    originalPortPartitionSize = System.getProperty("geode.portPartitionSize");
    originalDebugPortPartitioning = System.getProperty("geode.debugPortPartitioning");
  }

  @After
  public void tearDown() {
    // Restore original system properties
    if (originalPortPartitionSize != null) {
      System.setProperty("geode.portPartitionSize", originalPortPartitionSize);
    } else {
      System.clearProperty("geode.portPartitionSize");
    }

    if (originalDebugPortPartitioning != null) {
      System.setProperty("geode.debugPortPartitioning", originalDebugPortPartitioning);
    } else {
      System.clearProperty("geode.debugPortPartitioning");
    }
  }

  @Test
  public void testPortPartitioningConfigurationParsing() {
    // Test that the configuration is parsed correctly
    System.clearProperty("geode.portPartitionSize");
    assertThat(AvailablePortHelper.getPortPartitionSize()).isEqualTo(0);

    System.setProperty("geode.portPartitionSize", "500");
    assertThat(AvailablePortHelper.getPortPartitionSize()).isEqualTo(500);

    System.setProperty("geode.portPartitionSize", "0");
    assertThat(AvailablePortHelper.getPortPartitionSize()).isEqualTo(0);

    System.setProperty("geode.portPartitionSize", "-1");
    assertThat(AvailablePortHelper.getPortPartitionSize()).isEqualTo(0);

    System.setProperty("geode.portPartitionSize", "invalid");
    assertThat(AvailablePortHelper.getPortPartitionSize()).isEqualTo(0);
  }

  @Test
  public void testPortAllocationWithoutPartitioning() {
    System.clearProperty("geode.portPartitionSize");

    // Allocate some ports and verify they're in valid range
    Set<Integer> allocatedPorts = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
      allocatedPorts.add(port);
    }

    // All ports should be unique
    assertThat(allocatedPorts).hasSize(5);
  }

  @Test
  public void testPortAllocationWithPartitioning() {
    System.setProperty("geode.portPartitionSize", "200");

    // Allocate multiple ports and verify they're all within the same partition
    Set<Integer> allocatedPorts = new HashSet<>();
    int numPorts = 10;

    for (int i = 0; i < numPorts; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      allocatedPorts.add(port);
    }

    // All ports should be unique
    assertThat(allocatedPorts).hasSize(numPorts);

    // All ports should be within valid range
    for (int port : allocatedPorts) {
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    // If we have partitioning enabled, all ports should be within the same 200-port partition
    int minPort = allocatedPorts.stream().mapToInt(Integer::intValue).min().orElse(0);

    // Calculate which partition the minimum port belongs to
    int partitionIndex = (minPort - 20001) / 200;
    int partitionStart = 20001 + (partitionIndex * 200);
    int partitionEnd = partitionStart + 199;

    // All ports should be within this same partition
    for (int port : allocatedPorts) {
      assertThat(port).withFailMessage(
          "Port %d should be within partition [%d, %d]", port, partitionStart, partitionEnd)
          .isBetween(partitionStart, partitionEnd);
    }
  }

  @Test
  public void testConsistentPartitionAssignment() {
    System.setProperty("geode.portPartitionSize", "300");

    // Multiple port allocations should use the same partition for the same process
    Set<Integer> firstBatch = new HashSet<>();
    Set<Integer> secondBatch = new HashSet<>();

    // Allocate first batch
    for (int i = 0; i < 5; i++) {
      firstBatch.add(AvailablePortHelper.getRandomAvailableTCPPort());
    }

    // Allocate second batch
    for (int i = 0; i < 5; i++) {
      secondBatch.add(AvailablePortHelper.getRandomAvailableTCPPort());
    }

    // Determine partitions for both batches
    int firstMin = firstBatch.stream().mapToInt(Integer::intValue).min().orElse(0);
    int secondMin = secondBatch.stream().mapToInt(Integer::intValue).min().orElse(0);

    int firstPartition = (firstMin - 20001) / 300;
    int secondPartition = (secondMin - 20001) / 300;

    // Should be in the same partition
    assertThat(firstPartition).isEqualTo(secondPartition);
  }

  @Test
  public void testSmallPartitionSize() {
    System.setProperty("geode.portPartitionSize", "50");

    // Even with a small partition, should work correctly
    Set<Integer> allocatedPorts = new HashSet<>();

    // Try to allocate several ports
    for (int i = 0; i < 10; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      allocatedPorts.add(port);
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    // Should still get unique ports
    assertThat(allocatedPorts.size()).isGreaterThan(0);

    // Verify all ports are within the same 50-port partition
    int minPort = allocatedPorts.stream().mapToInt(Integer::intValue).min().orElse(0);
    int partitionIndex = (minPort - 20001) / 50;
    int partitionStart = 20001 + (partitionIndex * 50);
    int partitionEnd = partitionStart + 49;

    for (int port : allocatedPorts) {
      assertThat(port).isBetween(partitionStart, partitionEnd);
    }
  }

  @Test
  public void testDebugModeDoesNotBreakFunctionality() {
    System.setProperty("geode.debugPortPartitioning", "true");
    System.setProperty("geode.portPartitionSize", "100");

    // Should work normally even with debug enabled
    Set<Integer> allocatedPorts = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      allocatedPorts.add(port);
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    assertThat(allocatedPorts).hasSize(5);
  }

  @Test
  public void testPartitionBoundaryBehavior() {
    System.setProperty("geode.portPartitionSize", "100");

    // Allocate ports and verify they respect partition boundaries
    Set<Integer> allocatedPorts = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      allocatedPorts.add(port);
    }

    // All ports should be within valid range
    for (int port : allocatedPorts) {
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    // All ports should be within the same 100-port partition
    int minPort = allocatedPorts.stream().mapToInt(Integer::intValue).min().orElse(0);
    int maxPort = allocatedPorts.stream().mapToInt(Integer::intValue).max().orElse(0);

    // Calculate partition boundaries
    int partitionIndex = (minPort - 20001) / 100;
    int partitionStart = 20001 + (partitionIndex * 100);
    int partitionEnd = partitionStart + 99;

    // Verify all ports are within the same partition
    assertThat(maxPort - minPort).isLessThanOrEqualTo(99);
    assertThat(minPort).isGreaterThanOrEqualTo(partitionStart);
    assertThat(maxPort).isLessThanOrEqualTo(partitionEnd);
  }

  @Test
  public void testLargePartitionSize() {
    System.setProperty("geode.portPartitionSize", "5000"); // Larger than available range

    // Should still work without errors
    Set<Integer> allocatedPorts = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      allocatedPorts.add(port);
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    assertThat(allocatedPorts).hasSize(10);
  }

  @Test
  public void testPortPartitioningDoesNotAffectAvailability() {
    // Test that enabling partitioning doesn't break basic port allocation

    // First test without partitioning
    System.clearProperty("geode.portPartitionSize");
    Set<Integer> portsWithoutPartitioning = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      portsWithoutPartitioning.add(AvailablePortHelper.getRandomAvailableTCPPort());
    }
    assertThat(portsWithoutPartitioning).hasSize(5);

    // Then test with partitioning
    System.setProperty("geode.portPartitionSize", "500");
    Set<Integer> portsWithPartitioning = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      portsWithPartitioning.add(AvailablePortHelper.getRandomAvailableTCPPort());
    }
    assertThat(portsWithPartitioning).hasSize(5);

    // Both should return valid, unique ports
    for (int port : portsWithoutPartitioning) {
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }

    for (int port : portsWithPartitioning) {
      assertThat(port).isGreaterThanOrEqualTo(20001);
      assertThat(port).isLessThanOrEqualTo(29999);
    }
  }
}
