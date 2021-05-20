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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Encapsulates list containing objects, serialized objects, raw byte arrays, or exceptions. It can
 * optionally also hold the list of associated keys. Assumes that keys are either provided for all
 * entries or for none.
 *
 * @since GemFire 5.7
 */
public class ObjectPartList implements DataSerializableFixedID, Releasable {
  protected static final byte BYTES = 0;

  protected static final byte OBJECT = 1;

  protected static final byte EXCEPTION = 2;

  protected static final byte KEY_NOT_AT_SERVER = 3;

  protected byte[] objectTypeArray;

  protected boolean hasKeys;

  protected List<Object> keys;

  protected List<Object> objects;

  public void addPart(Object key, Object value, byte objectType, VersionTag versionTag) {
    int size = objects.size();
    int maxSize = objectTypeArray.length;
    if (size >= maxSize) {
      throw new IndexOutOfBoundsException("Cannot add object part beyond " + maxSize + " elements");
    }
    if (hasKeys) {
      if (key == null) {
        throw new IllegalArgumentException("Cannot add null key");
      }
      keys.add(key);
    }
    objectTypeArray[size] = objectType;
    objects.add(value);
  }

  // public methods

  public ObjectPartList() {
    objectTypeArray = null;
    hasKeys = false;
    keys = null;
    objects = new ArrayList<>();
  }

  public ObjectPartList(int maxSize, boolean hasKeys) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException(
          "Invalid size " + maxSize + " to ObjectPartList constructor");
    }
    objectTypeArray = new byte[maxSize];
    this.hasKeys = hasKeys;
    if (hasKeys) {
      keys = new ArrayList<>();
    } else {
      keys = null;
    }
    objects = new ArrayList<>();
  }

  public void addObjectPart(Object key, Object value, boolean isObject, VersionTag versionTag) {
    addPart(key, value, isObject ? OBJECT : BYTES, versionTag);
  }

  public void addExceptionPart(Object key, Exception ex) {
    addPart(key, ex, EXCEPTION, null);
  }

  public void addObjectPartForAbsentKey(Object key, Object value) {
    // ObjectPartList is for clients < version 6.5.0, which didn't support this setting
    throw new IllegalAccessError("inappropriate use of ObjectPartList");
  }



  public void addAll(ObjectPartList other) {
    if (hasKeys) {
      if (other.keys != null) {
        if (keys == null) {
          keys = new ArrayList<>(other.keys);
        } else {
          keys.addAll(other.keys);
        }
      }
    } else if (other.hasKeys) {
      hasKeys = true;
      keys = new ArrayList<>(other.keys);
    }
    objects.addAll(other.objects);
  }

  public List<Object> getKeys() {
    if (keys == null) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(keys);
    }
  }

  /** unprotected access to the keys collection, which may be null */
  @VisibleForTesting
  List<Object> getKeysForTest() {
    return keys;
  }

  public List<Object> getObjects() {
    if (objects == null) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(objects);
    }
  }

  /** unprotected access to the objects collection, which may be null */
  public List<Object> getObjectsForTest() {
    return objects;
  }

  public int size() {
    // some lists have only keys and some have only objects, so we need to choose
    // the correct collection to query
    if (hasKeys) {
      return keys.size();
    } else {
      return objects.size();
    }
  }

  public void reinit(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Invalid size " + maxSize + " to ObjectPartList.reinit");
    }
    objectTypeArray = new byte[maxSize];
    objects.clear();
    keys.clear();
  }

  public void clear() {
    release();
    objects.clear();
    if (keys != null) {
      keys.clear();
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeBoolean(hasKeys);
    if (objectTypeArray != null) {
      int numObjects = objects.size();
      out.writeInt(numObjects);
      for (int index = 0; index < numObjects; ++index) {
        Object value = objects.get(index);
        byte objectType = objectTypeArray[index];
        if (hasKeys) {
          context.getSerializer().writeObject(keys.get(index), out);
        }
        out.writeBoolean(objectType == EXCEPTION);
        if (objectType == OBJECT && value instanceof byte[]) {
          out.write((byte[]) value);
        } else if (objectType == EXCEPTION) {
          // write exception as byte array so native clients can skip it
          DataSerializer.writeByteArray(CacheServerHelper.serialize(value), out);
          // write the exception string for native clients
          DataSerializer.writeString(value.toString(), out);
        } else {
          context.getSerializer().writeObject(value, out);
        }
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    hasKeys = in.readBoolean();
    if (hasKeys) {
      keys = new ArrayList<>();
    }
    int numObjects = in.readInt();
    if (numObjects > 0) {
      for (int index = 0; index < numObjects; ++index) {
        if (hasKeys) {
          Object key = context.getDeserializer().readObject(in);
          keys.add(key);
        }
        boolean isException = in.readBoolean();
        Object value;
        if (isException) {
          byte[] exBytes = DataSerializer.readByteArray(in);
          value = CacheServerHelper.deserialize(exBytes);
          // ignore the exception string meant for native clients
          DataSerializer.readString(in);
        } else {
          value = context.getDeserializer().readObject(in);
        }
        objects.add(value);
      }
    }
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.OBJECT_PART_LIST;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public void release() {
    for (Object v : objects) {
      OffHeapHelper.release(v);
    }
  }
}
