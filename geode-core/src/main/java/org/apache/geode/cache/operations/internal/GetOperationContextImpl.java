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
package org.apache.geode.cache.operations.internal;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * This subclass's job is to keep customers from getting a reference to a value that is off-heap.
 * Any access to an off-heap value should appear to the customer as a serialized value.
 *
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class GetOperationContextImpl extends GetOperationContext implements Releasable {

  private boolean released;

  public GetOperationContextImpl(Object key, boolean postOperation) {
    super(key, postOperation);
  }

  /**
   * This method is for internal use and should not be on the public apis.
   */
  public @Unretained Object getRawValue() {
    return super.getValue();
  }

  @Override
  public Object getObject() {
    Object result = super.getObject();
    if (result instanceof StoredObject) {
      // For off-heap object act as if they are serialized forcing them to call getSerializedValue
      // or getValue
      result = null;
    }
    return result;
  }

  @Override
  public void setObject(Object value, boolean isObject) {
    released = false;
    super.setObject(value, isObject);
  }

  @Override
  public void setValue(Object value, boolean isObject) {
    released = false;
    super.setValue(value, isObject);
  }

  private void checkForReleasedOffHeapValue(StoredObject so) {
    // Note that we only care about stored objects with a ref count
    if (released && so.hasRefCount()) {
      throw new IllegalStateException(
          "Attempt to access off-heap value after the OperationContext callback returned.");
    }
  }

  @Override
  public byte[] getSerializedValue() {
    byte[] result = super.getSerializedValue();
    if (result == null) {
      Object v = super.getValue();
      if (v instanceof StoredObject) {
        StoredObject so = (StoredObject) v;
        checkForReleasedOffHeapValue(so);
        result = so.getValueAsHeapByteArray();
      }
    }
    return result;
  }

  @Override
  public Object getDeserializedValue() throws SerializationException {
    Object result = super.getDeserializedValue();
    if (result instanceof StoredObject) {
      StoredObject so = (StoredObject) result;
      checkForReleasedOffHeapValue(so);
      result = so.getValueAsDeserializedHeapObject();
    }
    return result;
  }

  @Override
  public Object getValue() {
    Object result = super.getValue();
    if (result instanceof StoredObject) {
      StoredObject so = (StoredObject) result;
      checkForReleasedOffHeapValue(so);
      // since they called getValue they don't care if it is serialized or deserialized so return it
      // as serialized
      result = so.getValueAsHeapByteArray();
    }
    return result;
  }

  @Override
  public void release() {
    // Note that if the context's value is stored off-heap
    // and release has been called then we do not release
    // our value (since this context did not retain it)
    // but we do make sure that any future attempt to access
    // the off-heap value fails.
    if (super.getValue() instanceof StoredObject) {
      released = true;
    }
  }

}
