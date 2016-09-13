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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Adds one more object type which indicates that the key is not present at the server.
 * 
 * [bruce] THIS CLASS IS OBSOLETE AS OF V7.0.  It is replaced with VersionedObjectList
 *  
 *
 */
public class ObjectPartList651 extends ObjectPartList {

  public ObjectPartList651 () {
    super();
  }
  /**
   * @param maximumchunksize
   * @param b
   */
  public ObjectPartList651(int maximumchunksize, boolean b) {
   super(maximumchunksize,b);
  }
  
  @Override
  public void addObjectPartForAbsentKey(Object key, Object value) {
    addPart(key, value, KEY_NOT_AT_SERVER, null);
  }
  
  @Override
  public void addAll(ObjectPartList other) {
    if (this.hasKeys) {
      this.keys.addAll(other.keys);
    } else if (other.hasKeys) {
      this.hasKeys = true;
      this.keys = other.keys;
    }

    for (int i = 0; i < other.objects.size(); i++) {
      this.objects.add(other.objects.get(i));
    }

    if (this.objectTypeArray != null) {
      byte[] temp = new byte[this.objectTypeArray.length];
      System.arraycopy(this.objectTypeArray, 0, temp, 0,
          this.objectTypeArray.length);
      this.objectTypeArray = new byte[this.objects.size()];
      System.arraycopy(temp, 0, this.objectTypeArray, 0, temp.length);
      System.arraycopy(other.objectTypeArray, 0, this.objectTypeArray,
          temp.length, other.objectTypeArray.length);
    } else {
      this.objectTypeArray = new byte[this.objects.size()];
      System.arraycopy(other.objectTypeArray, 0, this.objectTypeArray, 0,
          other.objectTypeArray.length);
    }
  }
  
  public boolean isKeyNotOnServer(int index) {
    return (this.objectTypeArray[index] == KEY_NOT_AT_SERVER);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeBoolean(this.hasKeys);
    if (this.objectTypeArray != null) {
      int numObjects = this.objects.size();
      out.writeInt(numObjects);
      for (int index = 0; index < numObjects; ++index) {
        Object value = this.objects.get(index);
        byte objectType = this.objectTypeArray[index];
        if (this.hasKeys) {
          DataSerializer.writeObject(this.keys.get(index), out);
        }
        if ((objectType == KEY_NOT_AT_SERVER)) {
          out.writeByte(KEY_NOT_AT_SERVER);
        } else if (objectType == EXCEPTION) {
          out.writeByte(EXCEPTION);
        } else {
          out.writeByte(OBJECT);
        }
        
        if (objectType == OBJECT && value instanceof byte[]) {
          out.write((byte[])value);
        }
        else if (objectType == EXCEPTION) {
          // write exception as byte array so native clients can skip it
          DataSerializer
              .writeByteArray(CacheServerHelper.serialize(value), out);
          // write the exception string for native clients
          DataSerializer.writeString(value.toString(), out);
        }
        else {
          DataSerializer.writeObject(value, out);
        }
      }
    }
    else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    boolean keysPresent = in.readBoolean();
    if (keysPresent) {
      this.keys = new ArrayList();
    }
    this.hasKeys = keysPresent;
    int numObjects = in.readInt();
    this.objectTypeArray = new byte[numObjects];
    if (numObjects > 0) {
      for (int index = 0; index < numObjects; ++index) {
        if (keysPresent) {
          Object key = DataSerializer.readObject(in);
          this.keys.add(key);
        }
        byte objectType = in.readByte();
        this.objectTypeArray[index] = objectType;
        Object value;
        if (objectType==EXCEPTION) {
          byte[] exBytes = DataSerializer.readByteArray(in);
          value = CacheServerHelper.deserialize(exBytes);
          // ignore the exception string meant for native clients
          DataSerializer.readString(in);
        }
        else {
          value = DataSerializer.readObject(in);
        }
        this.objects.add(value);
      }
    }
  }
  
  @Override
  public int getDSFID() {
    return DataSerializableFixedID.OBJECT_PART_LIST66;
  }
}
