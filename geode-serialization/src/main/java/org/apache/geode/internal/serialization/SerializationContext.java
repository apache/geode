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
 * A SerializationContext is passed to the toData() method of a DataSerializableFixedID
 * implementation. It can be used to obtain an ObjectSerializer and determine the
 * Geode Version of the destination of the data in cases where the SerializationVersions
 * interface is not being used to handle backward-compatibility issues. For instance,
 * you might decide to change a toData() method to check on the destination version
 * and decide to omit a field that you've added in a more recent version of Geode.
 */
public interface SerializationContext {

  /** return the version of the source/destination of this serializer */
  Version getSerializationVersion();

  /** return the serializer */
  ObjectSerializer getSerializer();

}
