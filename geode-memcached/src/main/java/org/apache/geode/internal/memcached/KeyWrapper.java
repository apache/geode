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
package org.apache.geode.internal.memcached;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

/**
 * Since byte[] cannot be used as keys in a Region/Map, instances of this class are used. Instances
 * of this class encapsulate byte[] keys and override equals and hashCode to base them on contents
 * on byte[].
 *
 */
public class KeyWrapper implements DataSerializable {

  private static final long serialVersionUID = -3241981993525734772L;

  /**
   * the key being wrapped
   */
  private byte[] key;

  public KeyWrapper() {}

  private KeyWrapper(byte[] key) {
    this.key = key;
  }

  /**
   * This method should be used to obtain instances of KeyWrapper.
   *
   * @param key the key to wrap
   * @return an instance of KeyWrapper that can be used as a key in Region/Map
   */
  public static KeyWrapper getWrappedKey(byte[] key) {
    return new KeyWrapper(key);
  }

  public byte[] getKey() {
    return key;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(key, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    key = DataSerializer.readByteArray(in);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KeyWrapper) {
      KeyWrapper other = (KeyWrapper) obj;
      return Arrays.equals(key, other.key);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName() + "@" + System.identityHashCode(this)
        + " key:" + Arrays.toString(key)
        + " hashCode:" + hashCode();
  }
}
