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

package org.apache.geode.cache.operations;

import org.apache.geode.DataSerializer;
import org.apache.geode.SerializationException;
import org.apache.geode.internal.cache.EntryEventImpl;


/**
 * Encapsulates a region operation that requires both key and serialized value for the pre-operation
 * and post-operation cases.
 *
 * @since GemFire 5.5
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public abstract class KeyValueOperationContext extends KeyOperationContext {

  /**
   * The value of the create/update operation.
   *
   * @since GemFire 6.5
   */
  private Object value;

  /**
   * True when the serialized object is a normal object; false when it is a raw byte array.
   */
  private boolean isObject;

  /**
   * Constructor for the operation.
   *
   * @param key the key for this operation
   * @param value the value for this operation
   * @param isObject true when the value is an object; false when it is a raw byte array
   * @since GemFire 6.5
   */
  public KeyValueOperationContext(Object key, Object value, boolean isObject) {
    super(key);
    setValue(value, isObject);
  }

  /**
   * Constructor for the operation.
   *
   * @param key the key for this operation
   * @param value the value for this operation
   * @param isObject true when the value is an object; false when it is a raw byte array
   * @param postOperation true if the context is at the time of sending updates
   * @since GemFire 6.5
   */
  public KeyValueOperationContext(Object key, Object value, boolean isObject,
      boolean postOperation) {
    super(key, postOperation);
    setValue(value, isObject);
  }

  /**
   * Get the serialized value for this operation.
   *
   * @return the serialized value for this operation or null if the value is not serialized
   */
  public byte[] getSerializedValue() {
    if (isObject()) {
      Object tmp = value;
      if (tmp instanceof byte[]) {
        return (byte[]) tmp;
      }
    }
    return null;
  }

  /**
   * Get the deserialized value for this operation. Note that if the value is serialized this method
   * will attempt to deserialize it. If PDX read-serialized is set to true and the value was
   * serialized with PDX then this method will return a PdxInstance.
   *
   * @return the deserialized value for this operation
   * @throws SerializationException if deserialization of the value fails
   * @since Geode 1.0
   */
  public Object getDeserializedValue() throws SerializationException {
    byte[] blob = getSerializedValue();
    if (blob != null) {
      return EntryEventImpl.deserialize(blob);
    }
    return value;
  }

  /**
   * Get the value for this operation. Note that if the value is serialized then a byte array will
   * be returned that contains the serialized bytes. To figure out if the returned byte array
   * contains serialized bytes or is the deserialized value call {@link #isObject()}. If you need to
   * deserialize the serialized bytes use {@link DataSerializer#readObject(java.io.DataInput)} or
   * you can just call {@link #getDeserializedValue()}.
   *
   * @return the value for this operation
   * @since GemFire 6.5
   */
  public Object getValue() {
    return value;
  }

  /**
   * Return true when the value is an object and not a raw byte array.
   *
   * @return true when the value is an object; false when it is a raw byte array
   */
  public boolean isObject() {
    return isObject;
  }

  /**
   * Set the serialized value object for this operation.
   *
   * @param serializedValue the serialized value for this operation
   * @param isObject true when the value is an object; false when it is a raw byte array
   */
  public void setSerializedValue(byte[] serializedValue, boolean isObject) {
    setValue(serializedValue, isObject);
  }


  /**
   * Set the result value of the object for this operation.
   *
   * @param value the result of this operation; can be a serialized byte array or a deserialized
   *        object
   * @param isObject true when the value is an object (either serialized or deserialized); false
   *        when it is a raw byte array
   * @since GemFire 6.5
   */
  public void setValue(Object value, boolean isObject) {
    this.value = value;
    this.isObject = isObject;
  }

}
