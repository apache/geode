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
 *
 */
package org.apache.geode.gradle.testing.isolation;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Establishes a distinct port range for each test worker JVM.
 */
public class PortRangeContext {
  private static final String AVAILABLE_PORT_LOWER_BOUND_PROPERTY = "AvailablePort.lowerBound";
  private static final String AVAILABLE_PORT_UPPER_BOUND_PROPERTY = "AvailablePort.upperBound";
  private static final String MEMBERSHIP_PORT_RANGE_PROPERTY = "gemfire.membership-port-range";

  /**
   * The full range of port numbers available for non-membership port use.
   * <p>
   * This range must match AvailablePort's default port range: [20001, 29999]
   */
  public static final PortRange AVAILABLE_PORT_FULL_RANGE = new PortRange(20001, 29999);

  /**
   * The full range of port numbers available for membership port use.
   * <p>
   * This range must match MembershipConfig.DEFAULT_MEMBERSHIP_PORT_RANGE: [41000, 61000]
   */
  public static final PortRange MEMBERSHIP_PORT_FULL_RANGE = new PortRange(41000, 61000);

  private final PortRange membershipPorts;
  private final PortRange availablePorts;

  private PortRangeContext(int index, int numberOfContexts) {
    membershipPorts = MEMBERSHIP_PORT_FULL_RANGE.partition(index, numberOfContexts);
    availablePorts = AVAILABLE_PORT_FULL_RANGE.partition(index, numberOfContexts);
  }

  /**
   * Return a list with the specified number of distinct port partitions.
   */
  public static List<PortRangeContext> create(int numberOfContexts) {
    return IntStream.range(0, numberOfContexts)
      .mapToObj(i -> new PortRangeContext(i, numberOfContexts))
      .collect(toList());
  }

  /**
   * Add this context's port ranges to the process builder as system properties.
   */
  public void configure(ProcessBuilder processBuilder) {
    List<String> command = processBuilder.command();
    command.add(1, String.format("-D%s=%d-%d", MEMBERSHIP_PORT_RANGE_PROPERTY,
      membershipPorts.lowerBound(), membershipPorts.upperBound()));
    command.add(1, String.format("-D%s=%d", AVAILABLE_PORT_LOWER_BOUND_PROPERTY,
      availablePorts.lowerBound()));
    command.add(1, String.format("-D%s=%d", AVAILABLE_PORT_UPPER_BOUND_PROPERTY,
      availablePorts.upperBound()));
  }

  @Override
  public String toString() {
    return "PortRangeContext{" +
      "availablePorts=" + availablePorts +
      ", membershipPorts=" + membershipPorts +
      '}';
  }
}
