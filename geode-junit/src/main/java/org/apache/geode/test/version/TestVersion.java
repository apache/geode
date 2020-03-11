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
package org.apache.geode.test.version;

import java.io.Serializable;
import java.util.Objects;

import org.apache.geode.internal.serialization.Version;

public class TestVersion implements Comparable, Serializable {
  public static final TestVersion CURRENT_VERSION = new TestVersion(VersionManager.CURRENT_VERSION);

  private final int major;
  private final int minor;
  private final int release;

  public static TestVersion valueOf(final String versionString) {
    return new TestVersion(versionString);
  }

  public TestVersion(String versionString) {
    String[] split = versionString.split("\\.");
    if (split.length != 3) {
      throw new IllegalArgumentException("Expected a version string but received " + versionString);
    }
    major = Integer.parseInt(split[0]);
    minor = Integer.parseInt(split[1]);
    if (split[2].contains("-incubating")) {
      split[2] = split[2].substring(0, split[2].length() - "-incubating".length());
    }
    release = Integer.parseInt(split[2]);
  }

  /**
   * Perform a comparison of the major, minor and patch versions of the two version strings.
   * The version strings should be in dot notation.
   */
  public static int compare(String version1, String version2) {
    return new TestVersion(version1).compareTo(new TestVersion(version2));
  }

  public boolean isSameAs(Version version) {
    if (equals(CURRENT_VERSION) && version.equals(Version.getCurrentVersion())) {
      return true;
    }
    return release == version.getRelease()
        && minor == version.getMinorVersion()
        && major == version.getMajorVersion();
  }

  @Override
  public String toString() {
    return "" + major + "." + minor + "." + release;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TestVersion)) {
      return false;
    }
    TestVersion that = (TestVersion) o;
    return major == that.major &&
        minor == that.minor &&
        release == that.release;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, release);
  }

  public TestVersion(int major, int minor, int release) {
    this.major = major;
    this.minor = minor;
    this.release = release;
  }

  @Override
  public int compareTo(Object o) {
    if (o == null) {
      throw new NullPointerException("parameter may not be null");
    }
    TestVersion other = (TestVersion) o;
    int comparison = Integer.compare(major, other.major);
    if (comparison != 0) {
      return comparison;
    }
    comparison = Integer.compare(minor, other.minor);
    if (comparison != 0) {
      return comparison;
    }
    return Integer.compare(release, other.release);
  }

  public int compareTo(int major, int minor, int patch) {
    return compareTo(new TestVersion(major, minor, patch));
  }

  public boolean lessThan(final TestVersion other) {
    return compareTo(other) < 0;
  }

  public boolean equals(final TestVersion other) {
    return compareTo(other) == 0;
  }

  public boolean greaterThan(final TestVersion other) {
    return compareTo(other) > 0;
  }

  public boolean lessThanOrEqualTo(final TestVersion other) {
    return !greaterThan(other);
  }

  public boolean greaterThanOrEqualTo(final TestVersion other) {
    return !lessThan(other);
  }
}
