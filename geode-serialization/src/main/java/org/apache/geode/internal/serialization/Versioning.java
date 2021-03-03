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
 * This is a factory for getting {@link Version} instances. It's aware of the whole
 * {link @Version} hierarchy, so when asked for a {@link Version} that represents
 * a known version ({@link KnownVersion}) it returns a reference to one of those.
 *
 * This ensures that {@link #toString()} on any {@Version}, if that object represents a
 * known version, will render itself as a such.
 */
public class Versioning {
  private Versioning() {}

  /**
   * Find the {@link Version} for the short ordinal value.
   *
   * If the short ordinal represents a known version ({@link KnownVersion}) then return
   * that instead of constructing a new {@link UnknownVersion}.
   *
   * @return a known version ({@link KnownVersion}) if possible, otherwise an
   *         {@link UnknownVersion}.
   */
  public static Version getVersion(final short ordinal) {
    final KnownVersion knownVersion = KnownVersion.getKnownVersion(ordinal);
    if (knownVersion == null) {
      return new UnknownVersion(ordinal);
    } else {
      return knownVersion;
    }
  }

  /**
   * Return the known version ({@link KnownVersion}) for {@code anyVersion}, if possible.
   * Otherwise return {@code defaultKnownVersion}. This method essentially
   * downcasts a {@link Version} to a known version {@link KnownVersion}.
   *
   * @param anyVersion came from a call to {@link #getVersion(short)}
   * @param defaultKnownVersion will be returned if {@code anyVersion} does not represent
   *        a known version
   * @return a known version
   */
  public static KnownVersion getKnownVersionOrDefault(final Version anyVersion,
      KnownVersion defaultKnownVersion) {
    if (anyVersion instanceof KnownVersion) {
      return (KnownVersion) anyVersion;
    } else {
      return defaultKnownVersion;
    }
  }

}
