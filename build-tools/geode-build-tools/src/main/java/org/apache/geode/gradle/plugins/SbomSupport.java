/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.gradle.plugins;

import org.gradle.util.GradleVersion;

/**
 * Decides whether the running Gradle version is new enough to generate SBOMs (GEODE-10481).
 *
 * <p>The version-comparison logic is kept here as a small, dependency-light unit so it can be
 * unit-tested without standing up a Gradle build. SBOM generation itself stays feature-flagged
 * and is wired up in later phases; this class only provides the compatibility gate.
 */
public final class SbomSupport {

  /**
   * Minimum Gradle version required to generate SBOMs with the CycloneDX 1.x plugin line that
   * Geode pins while it builds on Gradle 7.x.
   */
  public static final String MINIMUM_GRADLE_VERSION = "7.0";

  private SbomSupport() {
    // static utility
  }

  /**
   * Returns true if {@code currentVersion} is greater than or equal to {@code minimumVersion}.
   */
  public static boolean isGradleVersionSupported(String currentVersion, String minimumVersion) {
    return GradleVersion.version(currentVersion)
        .compareTo(GradleVersion.version(minimumVersion)) >= 0;
  }

  /**
   * Returns true if the Gradle version running this build satisfies {@link #MINIMUM_GRADLE_VERSION}.
   */
  public static boolean isCurrentGradleVersionSupported() {
    return GradleVersion.current()
        .compareTo(GradleVersion.version(MINIMUM_GRADLE_VERSION)) >= 0;
  }
}
