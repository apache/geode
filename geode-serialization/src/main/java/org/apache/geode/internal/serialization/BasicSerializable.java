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
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.
   * <p>
   * Since 5.7 it is possible for any method call to the specified <code>DataOutput</code> to throw
   * {@link GemFireRethrowable}. It should <em>not</em> be caught by user code. If it is it
   * <em>must</em> be rethrown.
   *
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  void toData(DataOutput out) throws IOException;

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  void fromData(DataInput in) throws IOException, ClassNotFoundException;
}
