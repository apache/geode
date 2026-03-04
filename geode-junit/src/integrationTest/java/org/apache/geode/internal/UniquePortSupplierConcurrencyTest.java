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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

/**
 * Test that demonstrates the race condition when multiple UniquePortSupplier instances
 * (simulating parallel test classes with --max-workers=12) allocate ports simultaneously.
 *
 * Uses a CONSTRAINED port range to dramatically increase collision probability.
 *
 * <p>
 * WITHOUT FIX: This test will show port collisions (same port allocated multiple times)
 * WITH FIX: This test will pass (all ports unique across instances)
 */
public class UniquePortSupplierConcurrencyTest {

  // Constrained port range to increase collision probability
  private static final int PORT_RANGE_START = 30000;
  private static final int PORT_RANGE_SIZE = 100; // 100 ports available

  @After
  public void cleanup() {
    UniquePortSupplier.clearGlobalCache();
  }

  @Test
  public void testMultipleInstancesCanCollideDuringParallelAllocation() throws Exception {
    // With 100 ports and 30 instances each allocating 2 ports (60 total),
    // WITHOUT FIX: collisions are highly likely due to lack of coordination
    // WITH FIX: all 60 ports can be successfully allocated without collision
    final int numInstances = 30;
    final int portsPerInstance = 2;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numInstances);

    final Map<Integer, AtomicInteger> portAllocationCount = new ConcurrentHashMap<>();

    ExecutorService executor = Executors.newFixedThreadPool(numInstances);

    try {
      // Simulate parallel "test classes", each creating its own UniquePortSupplier
      for (int i = 0; i < numInstances; i++) {
        executor.submit(() -> {
          try {
            startLatch.await(); // Wait for all threads to be ready

            // Each "test class" creates its own UniquePortSupplier with constrained port range
            Random random = new Random();
            UniquePortSupplier supplier = new UniquePortSupplier(
                () -> PORT_RANGE_START + random.nextInt(PORT_RANGE_SIZE));

            // Allocate ports
            for (int j = 0; j < portsPerInstance; j++) {
              int port = supplier.getAvailablePort();

              // Track how many times this port was allocated
              portAllocationCount.computeIfAbsent(port, k -> new AtomicInteger(0))
                  .incrementAndGet();
            }
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            doneLatch.countDown();
          }
        });
      }

      // Start all threads simultaneously to maximize concurrency
      startLatch.countDown();

      // Wait for completion
      boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
      assertThat(completed).as("All instances should complete").isTrue();

      // Analyze results
      System.out.println("\n========================================");
      System.out.println("=== Port Collision Test Results ===");
      System.out.println(
          "Port Range: " + PORT_RANGE_START + "-" + (PORT_RANGE_START + PORT_RANGE_SIZE - 1));
      System.out.println("Available ports: " + PORT_RANGE_SIZE);
      System.out.println("Instances: " + numInstances);
      System.out.println("Ports per instance: " + portsPerInstance);
      System.out.println("Total allocations: " + (numInstances * portsPerInstance));
      System.out.println("Unique ports allocated: " + portAllocationCount.size());

      List<Integer> collidedPorts = new ArrayList<>();
      int totalCollisions = 0;

      for (Map.Entry<Integer, AtomicInteger> entry : portAllocationCount.entrySet()) {
        int count = entry.getValue().get();
        if (count > 1) {
          collidedPorts.add(entry.getKey());
          totalCollisions += (count - 1);
        }
      }

      System.out.println("Ports with collisions: " + collidedPorts.size());
      System.out.println("Total collision instances: " + totalCollisions);

      if (!collidedPorts.isEmpty()) {
        System.out.println("\n❌ COLLISIONS DETECTED - Showing top colliders:");
        portAllocationCount.entrySet().stream()
            .filter(e -> e.getValue().get() > 1)
            .sorted((a, b) -> b.getValue().get() - a.getValue().get())
            .limit(20)
            .forEach(e -> System.out.println(
                "  Port " + e.getKey() + " allocated " + e.getValue().get() + " times"));
        System.out.println(
            "\nThis is the BUG! Each UniquePortSupplier instance has its own usedPorts HashSet.");
        System.out.println(
            "Multiple instances can allocate the same port because they don't coordinate.");
        System.out
            .println("The fix uses static GLOBAL_USED_PORTS to coordinate across all instances.");
      } else {
        System.out.println("\n✓ SUCCESS: No collisions - all ports unique");
        System.out
            .println("The fix is working: static GLOBAL_USED_PORTS coordinates across instances.");
      }
      System.out.println("========================================");

      // This assertion documents the EXPECTED behavior with/without fix:
      // WITHOUT FIX: This WILL fail because collisions occur with constrained port range
      // WITH FIX: This WILL pass because GLOBAL_USED_PORTS prevents collisions
      assertThat(collidedPorts)
          .as("No port should be allocated more than once (proves fix necessity)")
          .isEmpty();

    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
