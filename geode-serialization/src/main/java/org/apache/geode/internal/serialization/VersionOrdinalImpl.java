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

public class VersionOrdinalImpl implements VersionOrdinal {

  protected final short ordinal;

  /**
   * Package-private so only the Versioning factory can access this constructor.
   *
   */
  VersionOrdinalImpl(final short ordinal) {
    this.ordinal = ordinal;
  }

  @Override
  public short ordinal() {
    return ordinal;
  }

  @Override
  public int compareTo(final VersionOrdinal other) {
    if (other == null) {
      return 1;
    } else {
      return compareTo(other.ordinal());
    }
  }

  /**
   * TODO: eliminate this legacy method in favor of requiring callers to construct a
   * VersionOrdinalImpl. Inline this logic up in compareTo(VersionOrdinal).
   */
  public int compareTo(final short other) {
    // short min/max can't overflow int, so use (a-b)
    final int thisOrdinal = this.ordinal;
    final int otherOrdinal = other;
    return thisOrdinal - otherOrdinal;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof VersionOrdinalImpl) {
      return this.ordinal == ((VersionOrdinalImpl) other).ordinal;
    } else {
      return false;
    }
  }

  public boolean equals(final VersionOrdinal other) {
    return other != null && this.ordinal == other.ordinal();
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    result = mult * result + this.ordinal;
    return result;
  }

  @Override
  public String toString() {
    return toString(ordinal);
  }

  /**
   * TODO: eliminate this legacy method in favor of requiring callers to construct a
   * VersionOrdinalImpl. Inline this logic up in toString().
   */
  public static String toString(short ordinal) {
    return "VersionOrdinal[ordinal=" + ordinal + ']';
  }


  /**
   * Test if this version is older than given version.
   *
   * @param version to compare to this version
   * @return true if this is older than version, otherwise false.
   */
  @Override
  public final boolean isOlderThan(final VersionOrdinal version) {
    return compareTo(version) < 0;
  }

  /**
   * Test if this version is not older than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or newer, otherwise false.
   */
  @Override
  public final boolean isNotOlderThan(final VersionOrdinal version) {
    return compareTo(version) >= 0;
  }

  /**
   * Test if this version is newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is newer than version, otherwise false.
   */
  @Override
  public final boolean isNewerThan(final VersionOrdinal version) {
    return compareTo(version) > 0;
  }

  /**
   * Test if this version is not newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or older, otherwise false.
   */
  @Override
  public final boolean isNotNewerThan(final VersionOrdinal version) {
    return compareTo(version) <= 0;
  }

}
