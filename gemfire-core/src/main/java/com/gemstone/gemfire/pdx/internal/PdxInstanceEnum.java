/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalDataSerializer.Sendable;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializationException;
import com.gemstone.gemfire.pdx.WritablePdxInstance;
import com.gemstone.gemfire.pdx.internal.EnumInfo.PdxInstanceEnumInfo;
/**
 * Used to represent an enum value as a PdxInstance
 * @author darrel
 * @since 6.6.2
 */
public class PdxInstanceEnum implements PdxInstance, Sendable, ConvertableToBytes, ComparableEnum {
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
    this.className = e.getDeclaringClass().getName();
    this.enumName = e.name();
    this.enumOrdinal = e.ordinal();
  }
  
  public String getClassName() {
    return this.className;
  }
  public String getName() {
    return this.enumName;
  }

  public boolean isEnum() {
    return true;
  }
  public int getOrdinal() {
    return this.enumOrdinal;
  }

  @SuppressWarnings("unchecked")
  public Object getObject() {
    @SuppressWarnings("rawtypes")
    Class c;
    try {
      c = InternalDataSerializer.getCachedClass(this.className);
    } catch (ClassNotFoundException ex) {
      throw new PdxSerializationException(
              LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_A_CLASS_0
              .toLocalizedString(this.className), ex);
    }
    try {
      return Enum.valueOf(c, this.enumName);
    } catch (IllegalArgumentException ex) {
      throw new PdxSerializationException("Enum could not be deserialized because \"" + this.enumName + "\" is not a valid name in enum class " + c, ex);
    }
  }

  public boolean hasField(String fieldName) {
    return getFieldNames().contains(fieldName);
  }

  static private final List<String> fieldNames;
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
      return this.enumName;
    } else if ("ordinal".equals(fieldName)) {
      return this.enumOrdinal;
    }
    return null;
  }

  public WritablePdxInstance createWriter() {
    throw new IllegalStateException("PdxInstances that are an enum can not be modified.");
  }

  public void sendTo(DataOutput out) throws IOException {
    out.writeByte(DSCODE.PDX_INLINE_ENUM);
    DataSerializer.writeString(this.className, out);
    DataSerializer.writeString(this.enumName, out);
    InternalDataSerializer.writeArrayLength(this.enumOrdinal, out);
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ComparableEnum))
      return false;
    ComparableEnum other = (ComparableEnum) obj;
    if (className == null) {
      if (other.getClassName() != null)
        return false;
    } else if (!className.equals(other.getClassName()))
      return false;
    if (enumName == null) {
      if (other.getName() != null)
        return false;
    } else if (!enumName.equals(other.getName()))
      return false;
    return true;
  }
  
  @Override
  public String toString() {
    return this.enumName;
  }

  public byte[] toBytes() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
    sendTo(hdos);
    return hdos.toByteArray();
  }
  public int compareTo(Object o) {
    if (o instanceof ComparableEnum) {
      ComparableEnum other = (ComparableEnum)o;
      if (!getClassName().equals(other.getClassName())) {
        throw new ClassCastException("Can not compare a " + getClassName() + " to a " + other.getClassName());
      }
      return getOrdinal() - other.getOrdinal();
   } else {
      throw new ClassCastException("Can not compare an instance of " + o.getClass() + " to a " + this.getClass());
    }
  }
}
