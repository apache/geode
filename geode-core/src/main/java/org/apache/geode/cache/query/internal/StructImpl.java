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
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
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
  public StructImpl() {};

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
          this.hasPdx = true;
          break;
        }
      }
    }
  }

  /**
   * @throws IllegalArgumentException if fieldName not found
   */
  public Object get(String fieldName) {
    return this.values[this.type.getFieldIndex(fieldName)];
  }

  public ObjectType[] getFieldTypes() {
    return this.type.getFieldTypes();
  }


  public String[] getFieldNames() {
    return this.type.getFieldNames();
  }

  public Object[] getFieldValues() {
    if (this.values == null) {
      return new Object[0];
    }
    return this.values;
  }

  /**
   * Helper method, Returns field values, in case of PdxInstance gets the domain objects.
   */
  public Object[] getPdxFieldValues() {
    if (this.values == null) {
      return new Object[0];
    }

    Object[] fValues = new Object[this.values.length];
    for (int i = 0; i < this.values.length; i++) {
      if (this.values[i] instanceof PdxInstance) {
        fValues[i] = ((PdxInstance) this.values[i]).getObject();
      } else if (this.values[i] instanceof PdxString) {
        fValues[i] = ((PdxString) this.values[i]).toString();
      } else {
        fValues[i] = this.values[i];
      }
    }
    return fValues;
  }

  public StructType getStructType() {
    return this.type;
  }

  public boolean isHasPdx() {
    return hasPdx;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Struct))
      return false;
    Struct s = (Struct) obj;
    if (!Arrays.equals(getFieldTypes(), s.getStructType().getFieldTypes()))
      return false;
    if (!Arrays.equals(getFieldNames(), s.getStructType().getFieldNames()))
      return false;
    if (!Arrays.equals(getFieldValues(), s.getFieldValues()))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = this.type.hashCode();
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
    StringBuffer buf = new StringBuffer();
    buf.append("struct(");
    for (int i = 0; i < locValues.length; i++) {
      if (i > 0)
        buf.append(",");
      buf.append(names[i]);
      buf.append(":");
      buf.append(locValues[i]);
    }
    buf.append(")");
    return buf.toString();
  }

  public int getDSFID() {
    return STRUCT_IMPL;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.type = (StructTypeImpl) DataSerializer.readObject(in);
    this.values = DataSerializer.readObjectArray(in);
    if (this.values != null) {
      for (Object o : values) {
        if (o instanceof PdxInstance) {
          this.hasPdx = true;
          break;
        }
      }
    }
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.type, out);
    DataSerializer.writeObjectArray(this.values, out);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
