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
import java.io.Serializable;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxString;

/**
 * Implementation of Struct
 *
 * @since GemFire 4.0
 */
public class StructImpl implements Struct, DataSerializableFixedID, Serializable {
  private static final long serialVersionUID = -8474955084549542156L;
  private StructTypeImpl type;
  private Object[] values;
  // hasPdx will not be initialized by Java deserialization
  private transient boolean hasPdx = false;

  /** no-arg constructor required for DataSerializable */
  public StructImpl() {}

  /** Creates a new instance of StructImpl */
  public StructImpl(StructTypeImpl type, Object[] values) {
    if (type == null) {
      throw new IllegalArgumentException(
          "type must not be null");
    }
    this.type = type;
    this.values = values;
    if (this.values != null) {
      for (Object o : values) {
        if (o instanceof PdxInstance || o instanceof PdxString) {
          hasPdx = true;
          break;
        }
      }
    }
  }

  /**
   * @throws IllegalArgumentException if fieldName not found
   */
  @Override
  public Object get(String fieldName) {
    return values[type.getFieldIndex(fieldName)];
  }

  public ObjectType[] getFieldTypes() {
    return type.getFieldTypes();
  }


  public String[] getFieldNames() {
    return type.getFieldNames();
  }

  @Override
  public Object[] getFieldValues() {
    if (values == null) {
      return new Object[0];
    }
    return values;
  }

  /**
   * Helper method, Returns field values, in case of PdxInstance gets the domain objects.
   */
  public Object[] getPdxFieldValues() {
    if (values == null) {
      return new Object[0];
    }

    Object[] fValues = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof PdxInstance) {
        fValues[i] = ((PdxInstance) values[i]).getObject();
      } else if (values[i] instanceof PdxString) {
        fValues[i] = values[i].toString();
      } else {
        fValues[i] = values[i];
      }
    }
    return fValues;
  }

  @Override
  public StructType getStructType() {
    return type;
  }

  public boolean isHasPdx() {
    return hasPdx;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Struct)) {
      return false;
    }
    Struct s = (Struct) obj;
    if (!Arrays.equals(getFieldTypes(), s.getStructType().getFieldTypes())) {
      return false;
    }
    if (!Arrays.equals(getFieldNames(), s.getStructType().getFieldNames())) {
      return false;
    }
    return Arrays.equals(getFieldValues(), s.getFieldValues());
  }

  @Override
  public int hashCode() {
    int hashCode = type.hashCode();
    for (Object o : values) {
      if (o != null) {
        hashCode ^= o.hashCode();
      }
    }
    return hashCode;
  }

  @Override
  public String toString() {
    Object[] locValues = getFieldValues();
    String[] names = getFieldNames();
    StringBuilder buf = new StringBuilder();
    buf.append("struct(");
    for (int i = 0; i < locValues.length; i++) {
      if (i > 0) {
        buf.append(",");
      }
      buf.append(names[i]);
      buf.append(":");
      buf.append(locValues[i]);
    }
    buf.append(")");
    return buf.toString();
  }

  @Override
  public int getDSFID() {
    return STRUCT_IMPL;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    type = context.getDeserializer().readObject(in);
    values = DataSerializer.readObjectArray(in);
    if (values != null) {
      for (Object o : values) {
        if (o instanceof PdxInstance) {
          hasPdx = true;
          break;
        }
      }
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    context.getSerializer().writeObject(type, out);
    DataSerializer.writeObjectArray(values, out);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
