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
 * This interface is extended by DataSerializableFixedID and VersionedDataSerializable in order to
 * furnish version information to the serialization infrastructure for backward compatibility
 */

public interface SerializationVersions {
  /**
   * Returns the versions where this classes serialized form was modified. Versions returned by this
   * method are expected to be in increasing ordinal order from 0 .. N. For instance,<br>
   * {@link KnownVersion#GEODE_1_1_0}, {@link KnownVersion#GEODE_1_2_0}<br>
   * <p>
   * You are expected to implement toDataPre_GEODE_1_1_0_0(), fromDataPre_GEODE_1_1_0_0(),
   * toDataPre_GEODE_1_2_0_0, fromDataPre_GEODE_1_2_0_0.
   * <p>
   * The method name is formed with the version's product name and its major, minor, release and
   * patch numbers.
   */
  KnownVersion[] getSerializationVersions();

}
