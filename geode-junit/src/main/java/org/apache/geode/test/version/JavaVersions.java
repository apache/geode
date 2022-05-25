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

package org.apache.geode.test.version;

import static java.util.stream.Collectors.toList;
import static org.apache.logging.log4j.util.Strings.isBlank;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Discovers Java versions for use in {@link VmConfiguration}s. The known versions include:
 * <ul>
 * <li>The version of the current JVM.
 * <li>Each of the following if its specification version is lower than current JVM's and if the
 * corresponding environment variable identifies a directory:
 * <ul>
 * <li>Java 8: {@code TEST_JAVA_8_HOME}.</li>
 * <li>Java 11: {@code TEST_JAVA_11_HOME}.</li>
 * <li>Java 17: {@code TEST_JAVA_17_HOME}.</li>
 * </ul>
 * </li>
 * </ul>
 */
public class JavaVersions {
  private static final JavaVersion CURRENT_VERSION =
      new JavaVersion(currentJavaSpecificationVersion(), currentJavaHome());

  private static final Set<JavaVersion> KNOWN_VERSIONS = new HashSet<>();

  static {
    discover(8);
    discover(11);
    discover(17);
    add(CURRENT_VERSION, "java.home");
  }

  /**
   * @return a list of known Java versions
   */
  public static List<JavaVersion> all() {
    return KNOWN_VERSIONS.stream()
        .sorted()
        .collect(toList());
  }

  /**
   * @return the Java version of this JVM
   */
  public static JavaVersion current() {
    return CURRENT_VERSION;
  }

  /**
   * Returns a predicate that tests if its argument is less than {@code bound}.
   *
   * @param bound the upper bound
   * @return a predicate that tests if its argument is less than @{code bound}
   */
  public static Predicate<JavaVersion> lessThan(JavaVersion bound) {
    return v -> v.compareTo(bound) < 0;
  }

  /**
   * Returns a predicate that tests if its argument is at most {@code bound}.
   *
   * @param bound the upper bound
   * @return a predicate that tests if its argument is at most {@code bound}
   */
  public static Predicate<JavaVersion> atMost(JavaVersion bound) {
    return v -> v.compareTo(bound) <= 0;
  }

  /**
   * Returns a predicate that tests if its argument is at least {@code bound}.
   *
   * @param bound the lower bound
   * @return a predicate that tests if its argument is at least {@code bound}
   */
  public static Predicate<JavaVersion> atLeast(JavaVersion bound) {
    return v -> v.compareTo(bound) >= 0;
  }

  /**
   * Returns a predicate that tests if its argument is greater than {@code bound}.
   *
   * @param bound the lower bound
   * @return a predicate that tests if its argument is greater than {@code bound}
   */
  public static Predicate<JavaVersion> greaterThan(JavaVersion bound) {
    return v -> v.compareTo(bound) > 0;
  }

  private static void discover(int specificationVersion) {
    // Add the Java version only if its specification version is lower than the current JVM's.
    if (specificationVersion >= CURRENT_VERSION.specificationVersion()) {
      return;
    }
    String homeVariable = "TEST_JAVA_" + specificationVersion + "_HOME";
    String home = System.getenv(homeVariable);
    if (isBlank(home)) {
      return;
    }
    add(new JavaVersion(specificationVersion, Paths.get(home)), homeVariable);
  }

  private static void add(JavaVersion javaVersion, String source) {
    assertThat(javaVersion.home())
        .as("Java %d home from %s", javaVersion, source)
        .isDirectory();
    KNOWN_VERSIONS.add(javaVersion);
  }

  private static int currentJavaSpecificationVersion() {
    String specificationVersion = System.getProperty("java.specification.version");
    if (specificationVersion.contains(".")) {
      return Integer.parseInt(specificationVersion.split("\\.")[1]);
    }
    return Integer.parseInt(specificationVersion);
  }

  private static Path currentJavaHome() {
    return Paths.get(System.getProperty("java.home"));
  }
}
