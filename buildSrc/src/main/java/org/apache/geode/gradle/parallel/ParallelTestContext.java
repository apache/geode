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

package org.apache.geode.gradle.parallel;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.gradle.process.internal.JavaExecHandleBuilder;

/**
 * Establishes a distinct context for each test JVM, assigning each a distinct working directory and
 * a distinct set of ports.
 */
public class ParallelTestContext {
  private static final String GEMFIRE_PROPERTIES_FILENAME = "gemfire.properties";
  private static final String NON_MEMBERSHIP_PORT_LOWER_BOUND_PROPERTY = "AvailablePort.lowerBound";
  private static final String NON_MEMBERSHIP_PORT_UPPER_BOUND_PROPERTY = "AvailablePort.upperBound";
  private static final String MEMBERSHIP_PORT_LOWER_BOUND_PROPERTY =
      "AvailablePortHelper.membershipPortLowerBound";
  private static final String MEMBERSHIP_PORT_UPPER_BOUND_PROPERTY =
      "AvailablePortHelper.membershipPortUpperBound";

  /**
   * The unique sequence ID to assign to the next test context. The test context will configure the
   * name of the JVM's working directory to include this ID to make the working directory unique.
   */
  private static final AtomicInteger SEQUENCE_ID = new AtomicInteger();

  /**
   * The full range of port numbers available for non-membership port use.
   * <p>
   * This range must match AvailablePort's default port range: [20001, 29999]
   */
  public static final PortRange NON_MEMBERSHIP_PORT_FULL_RANGE = new PortRange(20001, 29999);

  /**
   * The full range of port numbers available for membership port use.
   * <p>
   * This range must match MembershipConfig.DEFAULT_MEMBERSHIP_PORT_RANGE: [41000, 61000]
   */
  public static final PortRange MEMBERSHIP_PORT_FULL_RANGE = new PortRange(41000, 61000);

  private final PortRange membershipPorts;
  private final PortRange nonMembershipPorts;

  private ParallelTestContext(int index, int numberOfContexts) {
    membershipPorts = MEMBERSHIP_PORT_FULL_RANGE.partition(index, numberOfContexts);
    nonMembershipPorts = NON_MEMBERSHIP_PORT_FULL_RANGE.partition(index, numberOfContexts);
  }

  /**
   * Return a list with the specified number of distinct test contexts.
   */
  public static List<ParallelTestContext> create(int numberOfContexts) {
    return IntStream.range(0, numberOfContexts)
        .mapToObj(i -> new ParallelTestContext(i, numberOfContexts))
        .collect(toList());
  }

  /**
   * Prepare the java command to launch a JVM that uses this context's working directory and ports.
   */
  public void configure(JavaExecHandleBuilder javaCommand) {
    Path taskWorkingDir = javaCommand.getWorkingDir().toPath();
    Path workerWorkingDir = taskWorkingDir.resolve(uniqueWorkingDirName());
    createWorkingDir(workerWorkingDir);
    copyGemFirePropertiesFile(taskWorkingDir, workerWorkingDir);
    javaCommand.setWorkingDir(workerWorkingDir);

    javaCommand.systemProperty(MEMBERSHIP_PORT_LOWER_BOUND_PROPERTY, membershipPorts.lowerBound());
    javaCommand.systemProperty(MEMBERSHIP_PORT_UPPER_BOUND_PROPERTY, membershipPorts.upperBound());
    javaCommand.systemProperty(NON_MEMBERSHIP_PORT_LOWER_BOUND_PROPERTY,
        String.valueOf(nonMembershipPorts.lowerBound()));
    javaCommand.systemProperty(NON_MEMBERSHIP_PORT_UPPER_BOUND_PROPERTY,
        String.valueOf(nonMembershipPorts.upperBound()));
  }

  private String uniqueWorkingDirName() {
    return String.format("test-worker-%05d", SEQUENCE_ID.getAndIncrement());
  }

  private void copyGemFirePropertiesFile(Path taskWorkingDir, Path workerWorkingDir) {
    Path taskPropertiesFile = taskWorkingDir.resolve(GEMFIRE_PROPERTIES_FILENAME);
    if (!Files.exists(taskPropertiesFile)) {
      return;
    }
    Path workerPropertiesFile = workerWorkingDir.resolve(taskPropertiesFile.getFileName());
    try {
      Files.copy(taskPropertiesFile, workerPropertiesFile, COPY_ATTRIBUTES);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void createWorkingDir(Path workerWorkingDir) {
    try {
      Files.createDirectories(workerWorkingDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
