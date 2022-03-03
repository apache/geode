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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface is not recommended for new work. It was introduced to solve some problems.
 * DataSerializableFixedID is the preferred interface for internal Geode messages. DataSerializable
 * should not be used for internal messaging. This is true for a number of reasons:
 *
 * <p>
 * <li>DataSerializableFixedID extends SerializationVersions so it has backward-compatibility
 * support built-in</li>
 * <li>DataSerializable is less efficient</li>
 * <li>DataSerializable is dependent on the (large) geode-core module</li>
 * </p>
 *
 * Geode Releases before 1.12 violated this rule: a number of internal messages implemented
 * DataSerializable. To correct that situation, this interface (BasicSerializable) was created. By
 * implementing the same on-the-wire format as DataSerializable, BasicSerializable provides a
 * compatibility bridge with older versions of Geode. The offending message classes were changed to
 * implement BasicSerializable.
 */
public interface BasicSerializable {

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.
   *
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  default void toData(DataOutput out, SerializationContext context) throws IOException {}

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  default void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {}

}
