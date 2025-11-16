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

import static org.apache.geode.internal.membership.utils.AvailablePort.SOCKET;
import static org.apache.geode.internal.membership.utils.AvailablePort.getRandomAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.internal.membership.utils.AvailablePort;

/**
 * Test to demonstrate the TOCTOU (Time-Of-Check-To-Time-Of-Use) race condition in port allocation.
 *
 * This test shows that even with static GLOBAL_USED_PORTS coordination (GEODE-10490 Layer 1 fix),
 * a race condition still exists between:
 * 1. Checking if port is available (getRandomAvailablePort)
 * 2. Actually binding to that port (ServerSocket.bind)
 *
 * The race window occurs because:
 * - getRandomAvailablePort() checks port availability by binding a socket
 * - The socket is immediately closed (releasing the port back to OS)
 * - Time gap exists before the test code binds to the port
 * - During this gap, OS or another thread can take the port
 *
 * This test simulates high parallelism (like CI with --max-workers=12) and demonstrates
 * that BindException can occur even with proper coordination between port allocation calls.
 *
 * The solution (Layer 2 fix) is to use the Keeper pattern which holds the socket
 * open until the caller is ready to use it.
 */
public class AvailablePortTOCTOURaceConditionTest {

  private final List<ServerSocket> socketsToClose = new ArrayList<>();
  private final List<AvailablePort.Keeper> keepersToRelease = new ArrayList<>();
  private ExecutorService executor;

  @After
  public void cleanup() {
    // Release all keepers first
    for (AvailablePort.Keeper keeper : keepersToRelease) {
      try {
        if (keeper != null) {
          keeper.release();
        }
      } catch (Exception ignored) {
      }
    }

    // Close all sockets created during test
    for (ServerSocket socket : socketsToClose) {
      try {
        if (socket != null && !socket.isClosed()) {
          socket.close();
        }
      } catch (IOException ignored) {
      }
    }

    if (executor != null) {
      executor.shutdownNow();
      try {
        executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Test that demonstrates the TOCTOU race condition.
   *
   * This test creates high parallelism with threads competing for ports.
   * It shows that even though getRandomAvailablePort() coordinates allocation,
   * BindException can still occur when there's a delay between allocation and binding.
   */
  @Test
  public void testTOCTOURaceCondition() throws Exception {
    int threadCount = 20; // Simulate high parallelism
    int attemptDelayMs = 10; // Introduce delay to increase race window

    executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(threadCount);

    AtomicInteger bindExceptions = new AtomicInteger(0);
    AtomicInteger successfulBinds = new AtomicInteger(0);
    Map<Integer, Integer> portUsageCount = new ConcurrentHashMap<>();

    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      Future<?> future = executor.submit(() -> {
        try {
          // Wait for all threads to be ready
          startLatch.await();

          // Get an available port
          int port = getRandomAvailablePort(SOCKET);
          portUsageCount.merge(port, 1, Integer::sum);

          // Introduce delay to simulate realistic time gap
          // In real tests, this gap occurs naturally due to:
          // - Test setup logic
          // - Object creation
          // - Configuration processing
          // - Other initialization steps
          Thread.sleep(attemptDelayMs);

          // Now try to bind to the port (simulating what LocatorStarterRule does)
          try {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(port));

            synchronized (socketsToClose) {
              socketsToClose.add(socket);
            }

            successfulBinds.incrementAndGet();

          } catch (BindException e) {
            // This is the TOCTOU race condition in action!
            // Port was available when checked, but taken by the time we tried to bind
            bindExceptions.incrementAndGet();
            System.err.println("Thread " + threadId + ": BindException on port " + port +
                " - " + e.getMessage());
          } catch (IOException e) {
            // Other IO errors
            System.err.println("Thread " + threadId + ": IOException on port " + port +
                " - " + e.getMessage());
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          completionLatch.countDown();
        }
      });
      futures.add(future);
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
    assertThat(completed).isTrue();

    // Print statistics
    System.out.println("\n=== TOCTOU Race Condition Test Results ===");
    System.out.println("Threads: " + threadCount);
    System.out.println("Successful binds: " + successfulBinds.get());
    System.out.println("Bind exceptions: " + bindExceptions.get());
    System.out.println("Total attempts: " + (successfulBinds.get() + bindExceptions.get()));

    // Check for ports that were attempted by multiple threads
    int portsWithMultipleAttempts = 0;
    for (Map.Entry<Integer, Integer> entry : portUsageCount.entrySet()) {
      if (entry.getValue() > 1) {
        portsWithMultipleAttempts++;
        System.out.println("Port " + entry.getKey() + " was attempted by " +
            entry.getValue() + " threads");
      }
    }

    System.out.println("Ports attempted by multiple threads: " + portsWithMultipleAttempts);
    System.out.println("===========================================\n");

    // This test demonstrates the issue - we expect to see:
    // 1. All threads get different ports (good coordination via GLOBAL_USED_PORTS)
    // 2. BUT: Some threads may still get BindException due to TOCTOU race

    // Verify that GLOBAL_USED_PORTS coordination is working
    // (all threads should get different ports)
    assertThat(portsWithMultipleAttempts)
        .as("GLOBAL_USED_PORTS should prevent multiple threads from getting same port")
        .isEqualTo(0);

    // However, we may still see BindExceptions due to OS taking ports during the delay
    // This is the TOCTOU race condition that needs to be fixed
    // We don't assert on bindExceptions count because it depends on OS behavior
    // and timing, but we log it to show the issue exists

    if (bindExceptions.get() > 0) {
      System.err.println("\n*** TOCTOU RACE CONDITION DETECTED ***");
      System.err
          .println("Even though each thread got a unique port from getRandomAvailablePort(),");
      System.err
          .println(bindExceptions.get() + " bind attempts failed due to 'Address already in use'.");
      System.err.println("This demonstrates the time gap between checking port availability");
      System.err.println("and actually binding allows OS or other processes to take the port.");
      System.err.println("****************************************\n");
    }
  }

  /**
   * Test that demonstrates the race by intentionally stealing ports.
   *
   * This test explicitly shows the TOCTOU window by having a "stealer" thread
   * that tries to bind to ports immediately after they're allocated.
   */
  @Test
  public void testTOCTOURaceWithIntentionalStealing() throws Exception {
    int victimThreads = 10;
    executor = Executors.newFixedThreadPool(victimThreads + 1); // +1 for stealer

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch allocationLatch = new CountDownLatch(victimThreads);
    CountDownLatch completionLatch = new CountDownLatch(victimThreads + 1);

    List<Integer> allocatedPorts = new ArrayList<>();
    AtomicInteger stolenPorts = new AtomicInteger(0);
    AtomicInteger bindExceptions = new AtomicInteger(0);

    // Victim threads: allocate ports with delay before binding
    for (int i = 0; i < victimThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();

          // Allocate port
          int port = getRandomAvailablePort(SOCKET);

          synchronized (allocatedPorts) {
            allocatedPorts.add(port);
          }

          allocationLatch.countDown();

          // Small delay - this is where port can be stolen
          Thread.sleep(50);

          // Try to bind
          try {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(port));

            synchronized (socketsToClose) {
              socketsToClose.add(socket);
            }

          } catch (BindException e) {
            bindExceptions.incrementAndGet();
            System.err.println("Thread " + threadId + ": Port " + port +
                " was stolen - " + e.getMessage());
          } catch (IOException e) {
            System.err.println("Thread " + threadId + ": IOException on port " + port +
                " - " + e.getMessage());
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          completionLatch.countDown();
        }
      });
    }

    // Stealer thread: aggressively try to bind to allocated ports
    executor.submit(() -> {
      try {
        startLatch.await();
        allocationLatch.await(); // Wait for victims to allocate ports

        Thread.sleep(25); // Give them a moment, but not enough

        // Try to steal the ports
        synchronized (allocatedPorts) {
          for (int port : allocatedPorts) {
            try {
              ServerSocket socket = new ServerSocket();
              socket.setReuseAddress(true);
              socket.bind(new InetSocketAddress(port));

              synchronized (socketsToClose) {
                socketsToClose.add(socket);
              }

              stolenPorts.incrementAndGet();
              System.out.println("Stealer: Successfully bound to port " + port);

            } catch (BindException e) {
              // Victim thread already bound to it
            } catch (IOException e) {
              // Other IO errors
            }
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        completionLatch.countDown();
      }
    });

    // Start all threads
    startLatch.countDown();

    // Wait for completion
    boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
    assertThat(completed).isTrue();

    System.out.println("\n=== Port Stealing Test Results ===");
    System.out.println("Ports allocated by victims: " + allocatedPorts.size());
    System.out.println("Ports stolen by stealer: " + stolenPorts.get());
    System.out.println("Bind exceptions from victims: " + bindExceptions.get());
    System.out.println("===================================\n");

    // If stealer successfully stole any ports, it demonstrates the TOCTOU race
    if (stolenPorts.get() > 0) {
      System.err.println("\n*** TOCTOU RACE CONDITION DEMONSTRATED ***");
      System.err.println("Stealer thread successfully bound to " + stolenPorts.get() +
          " ports that were allocated by victim threads.");
      System.err.println("This proves the time window exists between port allocation and binding.");
      System.err.println("*******************************************\n");
    }
  }

  /**
   * Test that demonstrates the Keeper pattern eliminates the TOCTOU race.
   *
   * This test uses getRandomAvailablePortKeeper() which returns a Keeper that holds
   * the socket open, preventing other threads from stealing the port during the time gap.
   */
  @Test
  public void testKeeperPatternPreventsRace() throws Exception {
    int victimThreads = 10;
    executor = Executors.newFixedThreadPool(victimThreads + 1); // +1 for stealer

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch allocationLatch = new CountDownLatch(victimThreads);
    CountDownLatch completionLatch = new CountDownLatch(victimThreads + 1);

    List<Integer> allocatedPorts = new ArrayList<>();
    AtomicInteger stolenPorts = new AtomicInteger(0);
    AtomicInteger bindExceptions = new AtomicInteger(0);
    AtomicInteger successfulBinds = new AtomicInteger(0);

    // Victim threads: allocate ports using Keeper pattern
    for (int i = 0; i < victimThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();

          // Allocate port using Keeper - this keeps the socket bound
          AvailablePort.Keeper keeper = AvailablePort.getRandomAvailablePortKeeper(SOCKET);
          if (keeper == null) {
            System.err.println("Thread " + threadId + ": Failed to get port keeper");
            return;
          }

          int port = keeper.getPort();

          synchronized (allocatedPorts) {
            allocatedPorts.add(port);
          }
          synchronized (keepersToRelease) {
            keepersToRelease.add(keeper);
          }

          allocationLatch.countDown();

          // Same delay as before - but now the port is held by Keeper
          Thread.sleep(50);

          // Release the keeper and immediately bind our own socket
          keeper.release();

          try {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(port));

            synchronized (socketsToClose) {
              socketsToClose.add(socket);
            }

            successfulBinds.incrementAndGet();

          } catch (BindException e) {
            bindExceptions.incrementAndGet();
            System.err.println("Thread " + threadId + ": Port " + port +
                " was stolen even with Keeper - " + e.getMessage());
          } catch (IOException e) {
            System.err.println("Thread " + threadId + ": IOException on port " + port +
                " - " + e.getMessage());
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          completionLatch.countDown();
        }
      });
    }

    // Stealer thread: try to steal ports (should fail with Keeper pattern)
    executor.submit(() -> {
      try {
        startLatch.await();
        allocationLatch.await(); // Wait for victims to allocate ports

        Thread.sleep(25); // Try to steal during the delay

        // Try to steal the ports - should fail because Keepers are holding them
        synchronized (allocatedPorts) {
          for (int port : allocatedPorts) {
            try {
              ServerSocket socket = new ServerSocket();
              socket.setReuseAddress(true);
              socket.bind(new InetSocketAddress(port));

              synchronized (socketsToClose) {
                socketsToClose.add(socket);
              }

              stolenPorts.incrementAndGet();
              System.err.println("Stealer: Successfully stole port " + port +
                  " (Keeper pattern failed!)");

            } catch (BindException e) {
              // Expected - Keeper is holding the port
              System.out
                  .println("Stealer: Failed to steal port " + port + " - Keeper prevented it");
            } catch (IOException e) {
              // Other IO errors
            }
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        completionLatch.countDown();
      }
    });

    // Start all threads
    startLatch.countDown();

    // Wait for completion
    boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
    assertThat(completed).isTrue();

    System.out.println("\n=== Keeper Pattern Test Results ===");
    System.out.println("Ports allocated by victims: " + allocatedPorts.size());
    System.out.println("Successful binds by victims: " + successfulBinds.get());
    System.out.println("Ports stolen by stealer: " + stolenPorts.get());
    System.out.println("Bind exceptions from victims: " + bindExceptions.get());
    System.out.println("====================================\n");

    // Verify that Keeper pattern prevented port stealing
    assertThat(stolenPorts.get())
        .as("Keeper pattern should prevent port stealing")
        .isEqualTo(0);

    assertThat(bindExceptions.get())
        .as("Victim threads should not experience bind exceptions with Keeper pattern")
        .isEqualTo(0);

    assertThat(successfulBinds.get())
        .as("All victim threads should successfully bind")
        .isEqualTo(victimThreads);

    System.out.println("\n*** KEEPER PATTERN SUCCESS ***");
    System.out.println("All " + victimThreads + " victim threads successfully allocated and bound");
    System.out.println("to ports without any race conditions or bind exceptions.");
    System.out.println("Stealer thread was unable to steal any ports (0/" + victimThreads + ").");
    System.out.println("This proves the Keeper pattern eliminates the TOCTOU race.");
    System.out.println("********************************\n");
  }
}
