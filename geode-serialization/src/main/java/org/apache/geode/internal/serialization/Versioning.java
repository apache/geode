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

  public static VersionOrdinal getVersionOrdinal(final short ordinal) {
    try {
      return Version.fromOrdinal(ordinal);
    } catch (final UnsupportedSerializationVersionException e) {
      return new VersionOrdinalImpl(ordinal);
    }
  }

}
