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

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Represents an installed version of Java.
 */
public class JavaVersion implements Comparable<JavaVersion>, Serializable {
  public final int specificationVersion;
  public final String home;

  public JavaVersion(int specificationVersion, Path home) {
    this.specificationVersion = specificationVersion;
    this.home = home.toAbsolutePath().normalize().toString();
  }

  /**
   * @return the absolute, normalized path to this version's home
   */
  public Path home() {
    return Paths.get(home);
  }

  /**
   * @return this Java version's specification version
   */
  public int specificationVersion() {
    return specificationVersion;
  }

  @Override
  public String toString() {
    return "" + specificationVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return specificationVersion == ((JavaVersion) o).specificationVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(specificationVersion);
  }

  @Override
  public int compareTo(JavaVersion other) {
    return Integer.compare(specificationVersion, other.specificationVersion);
  }
}
