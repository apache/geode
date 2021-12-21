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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.WritablePdxInstance;

public class EnumInfo implements DataSerializableFixedID {
  private String clazz;
  private String name;
  // The ordinal field is only used to support comparison.
  // It is not used for equals or hashcode.
  private int ordinal;
  private transient volatile WeakReference<Enum<?>> enumCache = null;

  public EnumInfo(Enum<?> e) {
    clazz = e.getDeclaringClass().getName();
    name = e.name();
    ordinal = e.ordinal();
    enumCache = new WeakReference<>(e);
  }

  public EnumInfo(String clazz, String name, int enumOrdinal) {
    this.clazz = clazz;
    this.name = name;
    ordinal = enumOrdinal;
  }

  public EnumInfo() {}

  @Override
  public int getDSFID() {
    return ENUM_INFO;
  }

  public String getClassName() {
    return clazz;
  }

  // This method is used by the "pdx rename" command.
  public void setClassName(String v) {
    clazz = v;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeString(clazz, out);
    DataSerializer.writeString(name, out);
    DataSerializer.writePrimitiveInt(ordinal, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    clazz = DataSerializer.readString(in);
    name = DataSerializer.readString(in);
    ordinal = DataSerializer.readPrimitiveInt(in);
  }

  public void flushCache() {
    synchronized (this) {
      enumCache = null;
    }
  }

  public int compareTo(EnumInfo other) {
    int result = clazz.compareTo(other.clazz);
    if (result == 0) {
      result = name.compareTo(other.name);
      if (result == 0) {
        result = other.ordinal - ordinal;
      }
    }
    return result;
  }

  public Enum<?> getEnum() throws ClassNotFoundException {
    Enum<?> result;
    if (InternalDataSerializer.LOAD_CLASS_EACH_TIME) {
      result = loadEnum();
    } else {
      result = getExistingEnum();
      if (result == null) {
        synchronized (this) {
          result = getExistingEnum();
          if (result == null) {
            result = loadEnum();
            enumCache = new WeakReference<>(result);
          }
        }
      }
    }
    return result;
  }

  public int getOrdinal() {
    return ordinal;
  }

  private Enum<?> getExistingEnum() {
    WeakReference<Enum<?>> wr = enumCache;
    if (wr != null) {
      return wr.get();
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Enum<?> loadEnum() throws ClassNotFoundException {
    @SuppressWarnings("rawtypes")
    Class c = InternalDataSerializer.getCachedClass(clazz);
    try {
      return Enum.valueOf(c, name);
    } catch (IllegalArgumentException ex) {
      throw new PdxSerializationException("PDX enum field could not be read because \"" + name
          + "\" is not a valid name in enum class " + c, ex);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return clazz + "." + name;
  }

  public String toFormattedString() {
    return getClass().getSimpleName() + "[\n        " + clazz + "." + name + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    EnumInfo other = (EnumInfo) obj;
    if (clazz == null) {
      if (other.clazz != null) {
        return false;
      }
    } else if (!clazz.equals(other.clazz)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (ordinal != other.ordinal) {
      throw new PdxSerializationException("The ordinal value for the enum " + name
          + " on class " + clazz
          + " can not be changed. Pdx only allows new enum constants to be added to the end of the enum.");
    }
    return true;
  }

  public PdxInstance getPdxInstance(int enumId) {
    return new PdxInstanceEnumInfo(enumId, this);
  }

  public static class PdxInstanceEnumInfo
      implements InternalPdxInstance, ComparableEnum {
    private static final long serialVersionUID = 7907582104525106416L;
    private final int enumId;
    private final EnumInfo ei;

    public PdxInstanceEnumInfo(int enumId, EnumInfo ei) {
      this.enumId = enumId;
      this.ei = ei;
    }

    @Override
    public String getClassName() {
      return ei.clazz;
    }

    @Override
    public String getName() {
      return ei.name;
    }

    @Override
    public int getOrdinal() {
      return ei.ordinal;
    }

    @Override
    public boolean isEnum() {
      return true;
    }

    @Override
    public Object getObject() {
      try {
        return ei.getEnum();
      } catch (ClassNotFoundException ex) {
        throw new PdxSerializationException(
            String.format("Could not create an instance of a class %s",
                getClassName()),
            ex);
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
        return getName();
      } else if ("ordinal".equals(fieldName)) {
        return getOrdinal();
      }
      return null;
    }

    @Override
    public WritablePdxInstance createWriter() {
      throw new IllegalStateException("PdxInstances that are an enum can not be modified.");
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      InternalDataSerializer.writePdxEnumId(enumId, out);
    }

    @Override
    public int hashCode() {
      // this hashCode needs to be kept consistent with PdxInstanceEnum
      final int prime = 31;
      int result = 1;
      String className = getClassName();
      String enumName = getName();
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
      String className = getClassName();
      if (className == null) {
        if (other.getClassName() != null) {
          return false;
        }
      } else if (!className.equals(other.getClassName())) {
        return false;
      }
      String enumName = getName();
      if (enumName == null) {
        return other.getName() == null;
      } else
        return enumName.equals(other.getName());
    }

    @Override
    public String toString() {
      return ei.name;
    }

    @Override
    public byte[] toBytes() throws IOException {
      HeapDataOutputStream hdos = new HeapDataOutputStream(16, KnownVersion.CURRENT);
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

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  public void toStream(PrintStream printStream) {
    printStream.print("  ");
    printStream.print(clazz);
    printStream.print('.');
    printStream.print(name);
    printStream.println();
  }
}
