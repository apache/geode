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

package org.apache.geode.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;


/**
 * This represents the CQ key value pair for the CQ query results.
 *
 * @since GemFire 6.5
 */

public class CqEntry implements DataSerializableFixedID {

  private Object key;

  private Object value;

  public CqEntry(Object key, Object value) {
    this.key = key;
    this.value = value;
  }

  /**
   * Constructor to read serialized data.
   */
  public CqEntry() {}

  /**
   * return key
   *
   * @return Object key
   */
  public Object getKey() {
    return key;
  }

  /**
   * return value
   *
   * @return Object value
   */
  public Object getValue() {
    return value;
  }

  /**
   * Returns key values as Object[] with key as the first and value as the last element of the
   * array.
   *
   * @return Object[] key, value pair
   */
  public Object[] getKeyValuePair() {
    return new Object[] {key, value};
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CqEntry)) {
      return false;
    }
    return key.equals(((CqEntry) other).getKey());
  }

  /* DataSerializableFixedID methods ---------------------------------------- */

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    key = context.getDeserializer().readObject(in);
    value = context.getDeserializer().readObject(in);
  }


  @Override
  public int getDSFID() {
    return CQ_ENTRY_EVENT;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(key, out);
    context.getSerializer().writeObject(value, out);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

}
