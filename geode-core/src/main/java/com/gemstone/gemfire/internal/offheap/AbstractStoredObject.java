/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.lang.StringUtils;

public abstract class AbstractStoredObject implements StoredObject {
  @Override
  public Object getValueAsDeserializedHeapObject() {
    return getDeserializedValue(null,null);
  }
  
  @Override
  public byte[] getValueAsHeapByteArray() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      return (byte[])getDeserializedForReading();
    }
  }

  @Override
  public String getStringForm() {
    try {
      return StringUtils.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  @Override
  public Object getDeserializedForReading() {
    return getDeserializedValue(null,null);
  }

  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return getDeserializedValue(null,null);
  }

  @Override
  public Object getValue() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      throw new IllegalStateException("Can not call getValue on StoredObject that is not serialized");
    }
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(getSerializedValue(), out);
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    if (isSerialized()) {
      out.write(getSerializedValue());
    } else {
      Object objToSend = (byte[]) getDeserializedForReading(); // deserialized as a byte[]
      DataSerializer.writeObject(objToSend, out);
    }
  }

  @Override
  public void sendAsByteArray(DataOutput out) throws IOException {
    byte[] bytes;
    if (isSerialized()) {
      bytes = getSerializedValue();
    } else {
      bytes = (byte[]) getDeserializedForReading();
    }
    DataSerializer.writeByteArray(bytes, out);
    
  }

  @Override
  public void sendAsCachedDeserializable(DataOutput out) throws IOException {
    if (!isSerialized()) {
      throw new IllegalStateException("sendAsCachedDeserializable can only be called on serialized StoredObjects");
    }
    InternalDataSerializer.writeDSFIDHeader(DataSerializableFixedID.VM_CACHED_DESERIALIZABLE, out);
    sendAsByteArray(out);
  }
  
  @Override
  public boolean usesHeapForStorage() {
    return false;
  }
  
  @Override
  public boolean isSerializedPdxInstance() {
    if (!isSerialized()) {
      return false;
    }
    byte dsCode = this.readDataByte(0);
    return dsCode == DSCODE.PDX || dsCode == DSCODE.PDX_ENUM || dsCode == DSCODE.PDX_INLINE_ENUM;
  }

  @Override
  public StoredObject getStoredObjectWithoutHeapForm() {
    // the only implementation that needs to override this
    // is OffHeapStoredObjectWithHeapForm.
    return this;
  }

}
