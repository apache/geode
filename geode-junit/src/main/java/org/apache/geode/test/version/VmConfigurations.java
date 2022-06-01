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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Methods to enumerate and evaluate VM configurations available for use in Geode tests.
 */
public class VmConfigurations {
  private static final List<VmConfiguration> ALL_CONFIGURATIONS = Stream.concat(
      currentGeodeWithEachJava(), currentJavaWithEachCompatibleOldGeode())
      .collect(toList());

  /**
   * Returns the list of available VM configurations. This includes:
   * <ul>
   * <li>Configurations for the current version of Java with each compatible version of Geode.</li>
   * <li>Configurations for the current version of Geode with each compatible version of Java.</li>
   * </ul>
   *
   * @return the list of available VM configurations
   */
  public static List<VmConfiguration> all() {
    return new ArrayList<>(ALL_CONFIGURATIONS);
  }

  /**
   * Returns the list of VM configurations suitable for use as starting configurations in upgrade
   * tests. Each upgrade configuration has either an old version of Geode or an old version of
   * Java, but not both.
   *
   * @return the list of VM configurations suitable for upgrade tests
   */
  public static List<VmConfiguration> upgrades() {
    return ALL_CONFIGURATIONS.stream()
        .filter(isUpgrade())
        .collect(toList());
  }

  /**
   * Returns a predicate that applies {@code predicate} to a configuration's Geode version.
   *
   * @param predicate a Geode version predicate
   * @return a predicate that applies {@code predicate} to a configuration's Geode version
   */
  public static Predicate<VmConfiguration> hasGeodeVersion(Predicate<TestVersion> predicate) {
    return config -> predicate.test(config.geodeVersion());
  }

  /**
   * Returns a predicate that applies {@code predicate} to a configuration's Java version.
   *
   * @param predicate a Java version predicate
   * @return a predicate that applies {@code predicate} to a configuration's Java version
   */
  public static Predicate<VmConfiguration> hasJavaVersion(Predicate<JavaVersion> predicate) {
    return config -> predicate.test(config.javaVersion());
  }

  private static Stream<VmConfiguration> currentGeodeWithEachJava() {
    return JavaVersions.all().stream()
        .map(VmConfiguration::forJavaVersion);
  }

  private static Stream<VmConfiguration> currentJavaWithEachCompatibleOldGeode() {
    return VersionManager.getInstance().getVersionsWithoutCurrent().stream()
        .map(TestVersion::valueOf)
        .filter(isCompatibleWithCurrentJava())
        .map(VmConfiguration::forGeodeVersion);
  }

  private static Predicate<VmConfiguration> isUpgrade() {
    return hasGeodeVersion(TestVersions.lessThan(TestVersion.current())).or(
        hasJavaVersion(JavaVersions.lessThan(JavaVersions.current())));
  }

  private static Predicate<TestVersion> isCompatibleWithCurrentJava() {
    return geodeVersion
    // Every version of Geode is compatible with Java 11 and below
    -> JavaVersions.current().specificationVersion() <= 11
        // Geode 1.15 and above are compatible with every Java version through Java 17
        || geodeVersion.greaterThanOrEqualTo(TestVersion.valueOf("1.15.0"));
  }
}
