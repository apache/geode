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


import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

import org.apache.geode.internal.membership.utils.AvailablePort.Keeper;

/**
 * Supplies unique ports that have not already been supplied by any instance of PortSupplier.
 * Uses a static shared set to coordinate port allocation across all test classes running in
 * parallel, preventing port collisions in highly parallel test environments.
 */
public class UniquePortSupplier {

  // Static shared set to prevent port collisions across all UniquePortSupplier instances
  // in parallel test execution (e.g., CI with --max-workers=12)
  private static final Set<Integer> GLOBAL_USED_PORTS = ConcurrentHashMap.newKeySet();

  private final IntSupplier supplier;

  public UniquePortSupplier() {
    supplier = AvailablePortHelper::getRandomAvailableTCPPort;
  }

  public UniquePortSupplier(IntSupplier supplier) {
    this.supplier = supplier;
  }

  public int getAvailablePort() {
    // Keep trying until we successfully claim a port that hasn't been claimed by another instance
    while (true) {
      int port = supplier.getAsInt();

      // Atomically add only if not already present
      if (GLOBAL_USED_PORTS.add(port)) {
        return port;
      }
      // If add returned false, port was already claimed by another instance, try again
    }
  }

  /**
   * Returns a Keeper that holds the port until released. This eliminates the TOCTOU race window
   * between port allocation and binding. The caller must call keeper.release() after binding.
   *
   * @return a Keeper object that holds the ServerSocket open
   */
  public Keeper getAvailablePortKeeper() {
    while (true) {
      Keeper keeper = AvailablePortHelper.getRandomAvailableTCPPortKeepers(1).get(0);
      int port = keeper.getPort();

      // Atomically add only if not already present
      if (GLOBAL_USED_PORTS.add(port)) {
        return keeper;
      }
      // If add returned false, port was already claimed by another instance,
      // release this keeper and try again
      keeper.release();
    }
  }

  /**
   * Clears the global cache of used ports. This is primarily for testing purposes to ensure
   * clean state between test runs.
   */
  static void clearGlobalCache() {
    GLOBAL_USED_PORTS.clear();
  }
}
