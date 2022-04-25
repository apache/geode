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

import static java.lang.String.format;
import static org.apache.geode.test.version.TestVersion.CURRENT_VERSION;

import java.io.Serializable;
import java.util.Objects;

/**
 * A configuration for a JVM used in a Geode test, defined by the version of Geode and the
 * version of Java.
 */
public class VmConfiguration implements Serializable {
  private final TestVersion geodeVersion;
  private final JavaVersion javaVersion;

  private VmConfiguration(JavaVersion javaVersion, TestVersion geodeVersion) {
    this.javaVersion = javaVersion;
    this.geodeVersion = geodeVersion;
  }

  /**
   * Returns a configuration for {@code geodeVersion} and the current JVM's version of Java.
   *
   * @param geodeVersion the Geode version for the configuration
   * @return the configuration
   */
  public static VmConfiguration forGeodeVersion(TestVersion geodeVersion) {
    return new VmConfiguration(JavaVersions.current(), geodeVersion);
  }

  /**
   * Returns a configuration for {@code geodeVersion} and the current JVM's version of Java.
   *
   * @param geodeVersion the Geode version for the configuration
   * @return the configuration
   */
  public static VmConfiguration forGeodeVersion(String geodeVersion) {
    return forGeodeVersion(TestVersion.valueOf(geodeVersion));
  }

  /**
   * Returns a configuration for {@code javaVersion} and the current JVM's version of Geode.
   *
   * @param javaVersion the Java version for the configuration
   * @return the configuration
   */
  public static VmConfiguration forJavaVersion(JavaVersion javaVersion) {
    return new VmConfiguration(javaVersion, CURRENT_VERSION);
  }

  /**
   * @return the configuration of the current JVM
   */
  public static VmConfiguration current() {
    return new VmConfiguration(JavaVersions.current(), CURRENT_VERSION);
  }

  /**
   * @return this configuration's Geode version
   */
  public TestVersion geodeVersion() {
    return geodeVersion;
  }

  /**
   * @return this configuration's Java version
   */
  public JavaVersion javaVersion() {
    return javaVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VmConfiguration that = (VmConfiguration) o;
    return geodeVersion.equals(that.geodeVersion) && javaVersion == that.javaVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(geodeVersion, javaVersion);
  }

  @Override
  public String toString() {
    return format("%s{java=%s, geode=%s}", getClass().getSimpleName(), javaVersion, geodeVersion);
  }
}
