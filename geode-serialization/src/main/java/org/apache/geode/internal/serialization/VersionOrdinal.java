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
 * VersionOrdinal is able to represent not only currently-known
 * Geode versions but future versions as well. This is necessary
 * because during rolling upgrades Geode manipulates member
 * identifiers for members running newer versions of the software.
 * In that case we receive the ordinal over the network
 * (serialization) but we don't know other version details such as
 * major/minor/patch version, which are known to the Version class.
 *
 * Implementations must define equals() and hashCode() based on
 * ordinal() result. And since this interface extends Comparable,
 * implementations must define compareTo() as well.
 *
 * Unlike Version (a subtype of VersionOrdinal which acts like an
 * enumerated type), VersionOrdinal does not, in general, guarantee
 * that if vo1.equals(vo2) then vo1 == vo2.
 *
 * Use the Versioning factory class to construct objects implementing
 * this interface. All instances of known versions are defined as
 * constants in the Version class, e.g. Version.GEODE_1_11_0
 */
public interface VersionOrdinal extends Comparable<VersionOrdinal> {

  /**
   * @return the short ordinal value for comparison implementations
   */
  public short ordinal();

  /*
   * What follows is a bunch of comparison methods phrased in terms of version age:
   * older/newer.
   */

  /**
   * Test if this version is older than given version.
   *
   * @param version to compare to this version
   * @return true if this is older than version, otherwise false.
   */
  boolean isOlderThan(Version version);

  /**
   * Test if this version is not older than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or newer, otherwise false.
   */
  boolean isNotOlderThan(Version version);

  /**
   * Test if this version is newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is newer than version, otherwise false.
   */
  boolean isNewerThan(Version version);

  /**
   * Test if this version is not newer than given version.
   *
   * @param version to compare to this version
   * @return true if this is the same version or older, otherwise false.
   */
  boolean isNotNewerThan(Version version);

}
