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
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.Version;
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
    this.clazz = e.getDeclaringClass().getName();
    this.name = e.name();
    this.ordinal = e.ordinal();
    this.enumCache = new WeakReference<Enum<?>>(e);
  }

  public EnumInfo(String clazz, String name, int enumOrdinal) {
    this.clazz = clazz;
    this.name = name;
    this.ordinal = enumOrdinal;
  }

  public EnumInfo() {}

  public int getDSFID() {
    return ENUM_INFO;
  }

  public String getClassName() {
    return this.clazz;
  }

  // This method is used by the "pdx rename" command.
  public void setClassName(String v) {
    this.clazz = v;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.clazz, out);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writePrimitiveInt(this.ordinal, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.clazz = DataSerializer.readString(in);
    this.name = DataSerializer.readString(in);
    this.ordinal = DataSerializer.readPrimitiveInt(in);
  }

  public void flushCache() {
    synchronized (this) {
      this.enumCache = null;
    }
  }

  public int compareTo(EnumInfo other) {
    int result = this.clazz.compareTo(other.clazz);
    if (result == 0) {
      result = this.name.compareTo(other.name);
      if (result == 0) {
        result = other.ordinal - this.ordinal;
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
            this.enumCache = new WeakReference<Enum<?>>(result);
          }
        }
      }
    }
    return result;
  }

  public int getOrdinal() {
    return this.ordinal;
  }

  private Enum<?> getExistingEnum() {
    WeakReference<Enum<?>> wr = this.enumCache;
    if (wr != null) {
      return wr.get();
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Enum<?> loadEnum() throws ClassNotFoundException {
    @SuppressWarnings("rawtypes")
    Class c = InternalDataSerializer.getCachedClass(this.clazz);
    try {
      return Enum.valueOf(c, this.name);
    } catch (IllegalArgumentException ex) {
      throw new PdxSerializationException("PDX enum field could not be read because \"" + this.name
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EnumInfo other = (EnumInfo) obj;
    if (clazz == null) {
      if (other.clazz != null)
        return false;
    } else if (!clazz.equals(other.clazz))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (this.ordinal != other.ordinal) {
      throw new PdxSerializationException("The ordinal value for the enum " + this.name
          + " on class " + this.clazz
          + " can not be changed. Pdx only allows new enum constants to be added to the end of the enum.");
    }
    return true;
  }

  public PdxInstance getPdxInstance(int enumId) {
    return new PdxInstanceEnumInfo(enumId, this);
  }

  public static class PdxInstanceEnumInfo
      implements PdxInstance, Sendable, ConvertableToBytes, ComparableEnum {
    private static final long serialVersionUID = 7907582104525106416L;
    private final int enumId;
    private final EnumInfo ei;

    public PdxInstanceEnumInfo(int enumId, EnumInfo ei) {
      this.enumId = enumId;
      this.ei = ei;
    }

    public String getClassName() {
      return this.ei.clazz;
    }

    public String getName() {
      return this.ei.name;
    }

    public int getOrdinal() {
      return this.ei.ordinal;
    }

    public boolean isEnum() {
      return true;
    }

    public Object getObject() {
      try {
        return this.ei.getEnum();
      } catch (ClassNotFoundException ex) {
        throw new PdxSerializationException(
            String.format("Could not create an instance of a class %s",
                getClassName()),
            ex);
      }
    }

    public boolean hasField(String fieldName) {
      return getFieldNames().contains(fieldName);
    }

    private static final List<String> fieldNames;
    static {
      ArrayList<String> tmp = new ArrayList<String>(2);
      tmp.add("name");
      tmp.add("ordinal");
      fieldNames = Collections.unmodifiableList(tmp);
    }

    public List<String> getFieldNames() {
      return fieldNames;
    }

    public boolean isIdentityField(String fieldName) {
      return false;
    }

    public Object getField(String fieldName) {
      if ("name".equals(fieldName)) {
        return getName();
      } else if ("ordinal".equals(fieldName)) {
        return getOrdinal();
      }
      return null;
    }

    public WritablePdxInstance createWriter() {
      throw new IllegalStateException("PdxInstances that are an enum can not be modified.");
    }

    public void sendTo(DataOutput out) throws IOException {
      InternalDataSerializer.writePdxEnumId(this.enumId, out);
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof ComparableEnum))
        return false;
      ComparableEnum other = (ComparableEnum) obj;
      String className = getClassName();
      if (className == null) {
        if (other.getClassName() != null)
          return false;
      } else if (!className.equals(other.getClassName()))
        return false;
      String enumName = getName();
      if (enumName == null) {
        if (other.getName() != null)
          return false;
      } else if (!enumName.equals(other.getName()))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return this.ei.name;
    }

    public byte[] toBytes() throws IOException {
      HeapDataOutputStream hdos = new HeapDataOutputStream(16, Version.CURRENT);
      sendTo(hdos);
      return hdos.toByteArray();
    }

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
            "Can not compare an instance of " + o.getClass() + " to a " + this.getClass());
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  public void toStream(PrintStream printStream) {
    printStream.print("  ");
    printStream.print(this.clazz);
    printStream.print('.');
    printStream.print(this.name);
    printStream.println();
  }
}
