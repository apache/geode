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

package org.apache.geode.internal.serialization;

/**
 * Extend this class to get short ordinal storage and access,
 * and comparison, hashing, and toString implementations.
 *
 * Package private since this class is an implementation detail.
 */
abstract class AbstractVersion implements Version {

  private final short ordinal;

  /**
   * Protected to require subclassing.
   */
  protected AbstractVersion(final short ordinal) {
    this.ordinal = ordinal;
  }

  @Override
  public short ordinal() {
    return ordinal;
  }

  @Override
  public int compareTo(final Version other) {
    if (other == null) {
      return 1;
    } else {
      return compareTo(other.ordinal());
    }
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof Version) {
      return ordinal() == ((Version) other).ordinal();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    result = mult * result + ordinal();
    return result;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[ordinal=" + ordinal() + ']';
  }

  /**
   * Test if this version is older than given version.
   *
   * @param version to compare to this version
   * @return true if this is older than version, otherwise false.
   */
  @Override
  public final boolean isOlderThan(final Version version) {
    return compareTo(version) < 0;
  }

  /**
   * Test if this version is not older than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or newer, otherwise false.
   */
  @Override
  public final boolean isNotOlderThan(final Version version) {
    return compareTo(version) >= 0;
  }

  /**
   * Test if this version is newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is newer than version, otherwise false.
   */
  @Override
  public final boolean isNewerThan(final Version version) {
    return compareTo(version) > 0;
  }

  /**
   * Test if this version is not newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or older, otherwise false.
   */
  @Override
  public final boolean isNotNewerThan(final Version version) {
    return compareTo(version) <= 0;
  }

  private int compareTo(final short other) {
    // short min/max can't overflow int, so use (a-b)
    final int thisOrdinal = ordinal();
    final int otherOrdinal = other;
    return thisOrdinal - otherOrdinal;
  }

}
