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
 * This interface supports toData/fromData serialization. DataSerializableFixedID is almost always
 * preferable to this interface for new work.
 */
public interface BasicSerializable {

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.<br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new toDataPre_GFE_X_X_X_X() method and add an entry for the current {@link
   * Version} in the getSerializationVersions array of the
   * implementing class. e.g. if msg format changed in version 80, create toDataPre_GFE_8_0_0_0, add
   * Version.GFE_80 to the getSerializationVersions array and copy previous toData contents to this
   * newly created toDataPre_GFE_X_X_X_X() method.
   *
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  default void toData(DataOutput out, SerializationContext context) throws IOException {};

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>. <br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new fromDataPre_GFE_X_X_X_X() method and add an entry for the current {@link
   * Version} in the getSerializationVersions array of the
   * implementing class. e.g. if msg format changed in version 80, create fromDataPre_GFE_8_0_0_0,
   * add Version.GFE_80 to the getSerializationVersions array and copy previous fromData contents to
   * this newly created fromDataPre_GFE_X_X_X_X() method.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  default void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {};
}
