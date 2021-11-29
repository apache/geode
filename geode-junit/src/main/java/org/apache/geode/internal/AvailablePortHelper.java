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

import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.membership.utils.AvailablePort.AVAILABLE_PORTS_LOWER_BOUND;
import static org.apache.geode.internal.membership.utils.AvailablePort.AVAILABLE_PORTS_UPPER_BOUND;
import static org.apache.geode.internal.membership.utils.AvailablePort.MULTICAST;
import static org.apache.geode.internal.membership.utils.AvailablePort.SOCKET;
import static org.apache.geode.internal.membership.utils.AvailablePort.getAddress;
import static org.apache.geode.internal.membership.utils.AvailablePort.isPortAvailable;
import static org.apache.geode.internal.membership.utils.AvailablePort.isPortKeepable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.geode.internal.membership.utils.AvailablePort.Keeper;

/**
 * Methods for acquiring one or more available ports. Despite "random" in the names, these methods
 * allocate ports in a round-robin fashion.
 */
public class AvailablePortHelper {
  // A test JVM will start at AVAILABLE_PORTS_LOWER_BOUND. Each child VM will call
  // initializeUniquePortRange() initialize nextCandidatePort to a unique value.
  private static final AtomicInteger nextCandidatePort =
      new AtomicInteger(AVAILABLE_PORTS_LOWER_BOUND);

  private AvailablePortHelper() {}

  /**
   * Returns an available tcp port.
   */
  public static int getRandomAvailableTCPPort() {
    return availablePort(SOCKET);
  }

  /**
   * Returns an available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    return availablePort(MULTICAST);
  }

  /**
   * Returns the requested number of available tcp ports.
   */
  public static int[] getRandomAvailableTCPPorts(int count) {
    return IntStream.generate(AvailablePortHelper::getRandomAvailableTCPPort)
        .limit(count)
        .toArray();
  }

  /**
   * Returns the requested number of consecutive available tcp ports.
   */
  public static int[] getRandomAvailableTCPPortRange(final int count) {
    int[] ports = new int[count];
    boolean needMorePorts = true;

    while (needMorePorts) {
      int base = nextCandidatePort.getAndUpdate(skipCandidatePorts(count));
      if (base + count > AVAILABLE_PORTS_UPPER_BOUND) {
        continue;
      }

      needMorePorts = false; // Assume we'll find enough in this batch
      for (int i = 0; i < count; i++) {
        int port = base + i;
        if (isPortAvailable(port, SOCKET, getAddress(SOCKET))) {
          ports[i] = port;
          continue;
        }
        needMorePorts = true;
        break;
      }
    }
    return ports;
  }

  /**
   * Returns the specified number of port keepers.
   */
  public static List<Keeper> getRandomAvailableTCPPortKeepers(int count) {
    return Stream.generate(AvailablePortHelper::availableKeeper)
        .limit(count)
        .collect(toList());
  }

  /**
   * Assign this JVM's next candidate port based on the given index. If each JVM in the same test
   * calls this function with a small, distinct number, the algorithm:
   * <ul>
   * <li>Separates the JVMs' next candidate ports reasonably well</li>
   * <li>Allows adding JVMs without needing to know the total number of JVMs ahead of time</li>
   * </ul>
   *
   * @param jvmIndex a small number different from that of any other JVM running in this test
   */
  public static void initializeUniquePortRange(int jvmIndex) {
    if (jvmIndex < 0) {
      throw new RuntimeException("Range number cannot be negative.");
    }

    if (jvmIndex == 0) {
      nextCandidatePort.set(AVAILABLE_PORTS_LOWER_BOUND);
      return;
    }

    // Generate starting points such that JVM 0 starts at the lower bound of the total port
    // range, JVM 1 starts halfway through, JVM 2 starts 1/4 of the way through, then further
    // ranges are 3/4, 1/8, 3/8, 5/8, 7/8, 1/16, etc.
    int availableRange = AVAILABLE_PORTS_UPPER_BOUND - AVAILABLE_PORTS_LOWER_BOUND;
    int numChunks = Integer.highestOneBit(jvmIndex) << 1;
    int chunkNumber = 2 * (jvmIndex - Integer.highestOneBit(jvmIndex)) + 1;
    int firstCandidatePort = AVAILABLE_PORTS_LOWER_BOUND + chunkNumber * availableRange / numChunks;

    nextCandidatePort.set(firstCandidatePort);
  }

  private static int availablePort(int protocol) {
    while (true) {
      int port = nextCandidatePort.getAndUpdate(skipCandidatePorts(1));
      if (port > AVAILABLE_PORTS_UPPER_BOUND) {
        continue;
      }

      if (isPortAvailable(port, protocol, getAddress(protocol))) {
        return port;
      }
    }
  }

  private static Keeper availableKeeper() {
    int count = 1;
    while (true) {
      int uniquePort = nextCandidatePort.getAndUpdate(skipCandidatePorts(1));
      if (uniquePort + count > AVAILABLE_PORTS_UPPER_BOUND) {
        continue;
      }
      Keeper keeper = isPortKeepable(uniquePort, SOCKET, getAddress(SOCKET));
      if (keeper != null) {
        return keeper;
      }
    }
  }

  private static IntUnaryOperator skipCandidatePorts(int n) {
    return port -> {
      int nextPort = port + n;
      if (nextPort <= AVAILABLE_PORTS_UPPER_BOUND) {
        return nextPort;
      }
      return AVAILABLE_PORTS_LOWER_BOUND;
    };
  }
}
