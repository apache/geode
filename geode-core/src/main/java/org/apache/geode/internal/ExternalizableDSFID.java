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
package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * Abstract class for subclasses that want to be Externalizable in addition to being
 * DataSerializableFixedID.
 * <p>
 * Note: subclasses must also provide a zero-arg constructor
 *
 * @since GemFire 5.7
 */
public abstract class ExternalizableDSFID implements DataSerializableFixedID, Externalizable {
  @Override
  public abstract int getDSFID();

  @Override
  public abstract void toData(DataOutput out) throws IOException;

  @Override
  public abstract void fromData(DataInput in) throws IOException, ClassNotFoundException;

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }
}
