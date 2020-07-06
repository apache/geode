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
 * This is a factory for getting VersionOrdinal instances. It's aware of the whole
 * VersionOrdinal/Version hierarchy, so when asked for a VersionOrdinal that represents
 * a known version (a Version) it returns a reference to one of those.
 *
 * This ensures that toString() on any VersionOrdinal, if that object represents a
 * known version, will render itself as a Version.
 */
public class Versioning {
  private Versioning() {}

  /**
   * Make a VersionOrdinal for the short ordinal value.
   *
   * If the short ordinal represents a known version (Version) then return
   * that instead of constructing a new VersionOrdinal.
   *
   * @return a known version (Version) if possible, otherwise a VersionOrdinal.
   */
  public static VersionOrdinal getVersionOrdinal(final short ordinal) {
    final Version knownVersion = Version.getKnownVersion(ordinal, null);
    if (knownVersion == null) {
      return new UnknownVersion(ordinal);
    } else {
      return knownVersion;
    }
  }

  /**
   * Return the known version (Version) for the VersionOrdinal, if possible.
   * Otherwise return the returnWhenUnknown Version. This method essentially
   * downcasts a {@link VersionOrdinal} to a known version {@link Version}
   *
   * @param anyVersion came from a call to {@link #getVersionOrdinal(short)} or this
   *        method
   * @param returnWhenUnknown will be returned if anyVersion does not represent
   *        a known version
   * @return a known version
   */
  public static Version getKnownVersion(final VersionOrdinal anyVersion,
      Version returnWhenUnknown) {
    if (anyVersion instanceof Version) {
      return (Version) anyVersion;
    } else {
      return returnWhenUnknown;
    }
  }

}
