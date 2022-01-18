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
package org.apache.geode.pdx.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.WritablePdxInstance;

/**
 * Used to represent an enum value as a PdxInstance
 *
 * @since GemFire 6.6.2
 */
public class PdxInstanceEnum implements InternalPdxInstance, ComparableEnum {
  private static final long serialVersionUID = -7417287878052772302L;
  private final String className;
  private final String enumName;
  private final int enumOrdinal;

  public PdxInstanceEnum(String className, String enumName, int enumOrdinal) {
    this.className = className;
    this.enumName = enumName;
    this.enumOrdinal = enumOrdinal;
  }

  public PdxInstanceEnum(Enum<?> e) {
    className = e.getDeclaringClass().getName();
    enumName = e.name();
    enumOrdinal = e.ordinal();
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return enumName;
  }

  @Override
  public boolean isEnum() {
    return true;
  }

  @Override
  public int getOrdinal() {
    return enumOrdinal;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getObject() {
    @SuppressWarnings("rawtypes")
    Class c;
    try {
      c = InternalDataSerializer.getCachedClass(className);
    } catch (ClassNotFoundException ex) {
      throw new PdxSerializationException(
          String.format("Could not create an instance of a class %s",
              className),
          ex);
    }
    try {
      return Enum.valueOf(c, enumName);
    } catch (IllegalArgumentException ex) {
      throw new PdxSerializationException("Enum could not be deserialized because \""
          + enumName + "\" is not a valid name in enum class " + c, ex);
    }
  }

  @Override
  public boolean hasField(String fieldName) {
    return getFieldNames().contains(fieldName);
  }

  @Immutable
  private static final List<String> fieldNames;
  static {
    ArrayList<String> tmp = new ArrayList<>(2);
    tmp.add("name");
    tmp.add("ordinal");
    fieldNames = Collections.unmodifiableList(tmp);
  }

  @Override
  public List<String> getFieldNames() {
    return fieldNames;
  }

  @Override
  public boolean isIdentityField(String fieldName) {
    return false;
  }

  @Override
  public Object getField(String fieldName) {
    if ("name".equals(fieldName)) {
      return enumName;
    } else if ("ordinal".equals(fieldName)) {
      return enumOrdinal;
    }
    return null;
  }

  @Override
  public WritablePdxInstance createWriter() {
    throw new IllegalStateException("PdxInstances that are an enum can not be modified.");
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    out.writeByte(DSCODE.PDX_INLINE_ENUM.toByte());
    DataSerializer.writeString(className, out);
    DataSerializer.writeString(enumName, out);
    InternalDataSerializer.writeArrayLength(enumOrdinal, out);
  }

  @Override
  public int hashCode() {
    // this hashCode needs to be kept consistent with EnumInfo.PdxInstanceEnumInfo
    final int prime = 31;
    int result = 1;
    result = prime * result + ((className == null) ? 0 : className.hashCode());
    result = prime * result + ((enumName == null) ? 0 : enumName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ComparableEnum)) {
      return false;
    }
    ComparableEnum other = (ComparableEnum) obj;
    if (className == null) {
      if (other.getClassName() != null) {
        return false;
      }
    } else if (!className.equals(other.getClassName())) {
      return false;
    }
    if (enumName == null) {
      return other.getName() == null;
    } else
      return enumName.equals(other.getName());
  }

  @Override
  public String toString() {
    return enumName;
  }

  @Override
  public byte[] toBytes() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    sendTo(hdos);
    return hdos.toByteArray();
  }

  @Override
  public int compareTo(Object o) {
    if (o instanceof ComparableEnum) {
      ComparableEnum other = (ComparableEnum) o;
      if (!getClassName().equals(other.getClassName())) {
        throw new ClassCastException(
            "Can not compare a " + getClassName() + " to a " + other.getClassName());
      }
      return getOrdinal() - other.getOrdinal();
    } else {
      throw new ClassCastException(
          "Can not compare an instance of " + o.getClass() + " to a " + getClass());
    }
  }
}
