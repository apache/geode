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

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.Version;

/**
 * Event that is persisted in HDFS. As we need to persist some of the EntryEventImpl
 * variables, we have created this class and have overridden toData and fromData functions.  
 * 
 *  There are subclasses of this class of the different types of persisted events
 *  sorted vs. unsorted, and the persisted events we keep in the region
 *  queue, which need to hold the region key.
 *   
 *
 * @author Hemant Bhanawat
 */
public abstract class PersistedEventImpl {
  protected Operation op = Operation.UPDATE;
  
  protected Object valueObject;

  /**
   * A field with flags decribing the event
   */
  protected byte flags;

   //FLags indicating the type of value
   //if the value is not a byte array or object, is is an internal delta.
  private static final byte VALUE_IS_BYTE_ARRAY= 0x01;
  private static final byte VALUE_IS_OBJECT= (VALUE_IS_BYTE_ARRAY << 1);
  private static final byte POSSIBLE_DUPLICATE = (VALUE_IS_OBJECT << 1);
  private static final byte HAS_VERSION_TAG = (POSSIBLE_DUPLICATE << 1);
  

  /** for deserialization */
  public PersistedEventImpl() {
  }
  
  public PersistedEventImpl(Object value, Operation op, byte valueIsObject,
      boolean isPossibleDuplicate, boolean hasVersionTag) throws IOException,
      ClassNotFoundException {
    this.op = op;
    this.valueObject = value;
    setFlag(VALUE_IS_BYTE_ARRAY, valueIsObject == 0x00);
    setFlag(VALUE_IS_OBJECT, valueIsObject == 0x01);
    setFlag(POSSIBLE_DUPLICATE, isPossibleDuplicate);
    setFlag(HAS_VERSION_TAG, hasVersionTag);
  }
  
  private void setFlag(byte flag, boolean set) {
    flags = (byte) (set ?  flags | flag :  flags & ~flag);
  }
  
  private boolean getFlag(byte flag) {
    return (flags & flag) != 0x0;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeByte(this.op.ordinal);
    out.writeByte(this.flags);
    
    if (getFlag(VALUE_IS_BYTE_ARRAY)) { 
      DataSerializer.writeByteArray((byte[])this.valueObject, out);
    } else if (getFlag(VALUE_IS_OBJECT)) {
      if(valueObject instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable)valueObject;
        DataSerializer.writeObjectAsByteArray(cd.getValue(), out);
      } else {
        DataSerializer.writeObjectAsByteArray(valueObject, out);
      }
    }
    else {
      DataSerializer.writeObject(valueObject, out);
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.op = Operation.fromOrdinal(in.readByte());
    this.flags = in.readByte();
    
    if (getFlag(VALUE_IS_BYTE_ARRAY)) { 
      this.valueObject = DataSerializer.readByteArray(in);
    } else if (getFlag(VALUE_IS_OBJECT)) {
      byte[] newValueBytes = DataSerializer.readByteArray(in);
      if(newValueBytes == null) {
        this.valueObject = null;
      } else {
        if(CachedDeserializableFactory.preferObject()) {
          this.valueObject =  EntryEventImpl.deserialize(newValueBytes);
        } else {
          this.valueObject = CachedDeserializableFactory.create(newValueBytes);
        }
      }
    }
    else {
      this.valueObject = DataSerializer.readObject(in);
    }
    
  }
  
  /**
   * Return the timestamp of this event. Depending on the subclass,
   * this may be part of the version tag, or a separate field.
   */
  public abstract long getTimstamp();

  protected boolean hasVersionTag() {
    return getFlag(HAS_VERSION_TAG);
  }

  public Operation getOperation()
  {
    return this.op;
  }
  
  public Object getValue() {
    return this.valueObject;
  }
  
  public boolean isPossibleDuplicate()
  {
    return getFlag(POSSIBLE_DUPLICATE);
  }

  /**
   * returns deserialized value. 
   * 
   */
  public Object getDeserializedValue() throws IOException, ClassNotFoundException {
    Object retVal = null;
    if (getFlag(VALUE_IS_BYTE_ARRAY)) { 
      // value is a byte array
      retVal = this.valueObject;
    } else if (getFlag(VALUE_IS_OBJECT)) {
      if(valueObject instanceof CachedDeserializable) {
        retVal = ((CachedDeserializable)valueObject).getDeserializedForReading();
      } else {
        retVal = valueObject;
      }
    }
    else {
      // value is a object
      retVal = this.valueObject;
    }
    return retVal;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(PersistedEventImpl.class.getSimpleName());
    str.append("@").append(System.identityHashCode(this))
    .append(" op:").append(op)
    .append(" valueObject:").append(valueObject)
    .append(" isPossibleDuplicate:").append(getFlag(POSSIBLE_DUPLICATE));
    return str.toString();
  }

  public void copy(PersistedEventImpl usersValue) {
    this.op = usersValue.op;
    this.valueObject = usersValue.valueObject;
    this.flags = usersValue.flags;
  }
  
  public static int getSizeInBytes(int keySize, int valueSize, VersionTag versionTag) {
    int size = 0;
    
    // value length
    size += valueSize; 

    // one byte for op and one byte for flag
    size += 2;
    
    return size;
  }
}
