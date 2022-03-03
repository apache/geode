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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.pdx.FieldType;

public class PdxField implements DataSerializable, Comparable<PdxField> {

  private static final long serialVersionUID = -1095459461236458274L;

  private String fieldName;
  private int fieldIndex;
  private int varLenFieldSeqId;
  private FieldType type;
  /**
   * If >= 0 then it is relative to the first byte of field data. Otherwise it is relative to the
   * base determined by vlfOffsetIndex.
   */
  private int relativeOffset;

  /**
   * if >= 0 then it is the index of the vlfOffsets that this field should use as its base to find
   * its data. If < 0 then it should be -1 which means the base is the first byte after the last
   * byte of field data.
   */
  private int vlfOffsetIndex;

  private boolean identityField;

  /**
   * Set to true by the pdx delete-field gfsh command
   *
   * @since GemFire 8.1
   */
  private boolean deleted;

  public PdxField() {}

  public PdxField(String fieldName, int index, int varId, FieldType type, boolean identityField) {
    this.fieldName = fieldName;
    fieldIndex = index;
    varLenFieldSeqId = varId;
    this.type = type;
    this.identityField = identityField;
  }

  /**
   * Used by {@link PdxInstanceImpl#equals(Object)} to act as if it has a field whose value is
   * always the default.
   */
  protected PdxField(PdxField other) {
    fieldName = other.fieldName;
    fieldIndex = other.fieldIndex;
    varLenFieldSeqId = other.varLenFieldSeqId;
    type = other.type;
    identityField = other.identityField;
    deleted = other.deleted;
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  public int getVarLenFieldSeqId() {
    return varLenFieldSeqId;
  }

  public boolean isVariableLengthType() {
    return !type.isFixedWidth();
  }

  public FieldType getFieldType() {
    return type;
  }

  public int getRelativeOffset() {
    return relativeOffset;
  }

  public void setRelativeOffset(int relativeOffset) {
    this.relativeOffset = relativeOffset;
  }

  public int getVlfOffsetIndex() {
    return vlfOffsetIndex;
  }

  public void setVlfOffsetIndex(int vlfOffsetIndex) {
    this.vlfOffsetIndex = vlfOffsetIndex;
  }

  public void setIdentityField(boolean identityField) {
    this.identityField = identityField;
  }

  public boolean isIdentityField() {
    return identityField;
  }

  public void setDeleted(boolean v) {
    deleted = v;
  }

  public boolean isDeleted() {
    return deleted;
  }

  private static final byte IDENTITY_BIT = 1;
  private static final byte DELETED_BIT = 2;

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    fieldName = DataSerializer.readString(in);
    fieldIndex = in.readInt();
    varLenFieldSeqId = in.readInt();
    type = DataSerializer.readEnum(FieldType.class, in);
    relativeOffset = in.readInt();
    vlfOffsetIndex = in.readInt();
    {
      byte bits = in.readByte();
      identityField = (bits & IDENTITY_BIT) != 0;
      deleted = (bits & DELETED_BIT) != 0;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(fieldName, out);
    out.writeInt(fieldIndex);
    out.writeInt(varLenFieldSeqId);
    DataSerializer.writeEnum(type, out);
    out.writeInt(relativeOffset);
    out.writeInt(vlfOffsetIndex);
    {
      // pre 8.1 we wrote a single boolean
      // 8.1 and after we write a byte whose bits are:
      // 1: identityField
      // 2: deleted
      byte bits = 0;
      if (identityField) {
        bits |= IDENTITY_BIT;
      }
      // Note that this code attempts to only set the DELETED_BIT
      // if serializing for 8.1 or later.
      // But in some cases 8.1 serialized data may be sent to a pre 8.1 member.
      // In that case if this bit is set it will cause the pre 8.1 member
      // to set identityField to true.
      // For this reason the pdx delete-field command should only be used after
      // all member have been upgraded to 8.1 or later.
      KnownVersion sourceVersion = StaticSerialization.getVersionForDataStream(out);
      if (sourceVersion.isNotOlderThan(KnownVersion.GFE_81)) {
        if (deleted) {
          bits |= DELETED_BIT;
        }
      }
      out.writeByte(bits);
    }
  }

  @Override
  public int hashCode() {
    int hash = 1;
    if (fieldName != null) {
      hash = hash * 31 + fieldName.hashCode();
    }
    if (type != null) {
      hash = hash * 31 + type.hashCode();
    }

    return hash;
  }

  // We don't compare the offsets here, because
  // this method is to see if two different PDXTypes
  // have equivalent fields. See PdxReaderImpl.equals.
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof PdxField)) {
      return false;
    }
    PdxField otherVFT = (PdxField) other;

    if (otherVFT.fieldName == null) {
      return false;
    }

    return otherVFT.fieldName.equals(fieldName) && isDeleted() == otherVFT.isDeleted()
        && type.equals(otherVFT.type);
  }

  @Override
  public String toString() {
    return fieldName + ":" + type + (isDeleted() ? ":DELETED" : "")
        + (isIdentityField() ? ":identity" : "") + ":" + fieldIndex
        + ((varLenFieldSeqId > 0) ? (":" + varLenFieldSeqId) : "")
        + ":idx0(relativeOffset)=" + relativeOffset + ":idx1(vlfOffsetIndex)="
        + vlfOffsetIndex;
  }

  public String getTypeIdString() {
    return getFieldType().toString();
  }

  @Override
  public int compareTo(PdxField o) {
    return getFieldName().compareTo(o.getFieldName());
  }

  public void toStream(PrintStream printStream) {
    printStream.print("    ");
    printStream.print(getFieldType());
    printStream.print(' ');
    printStream.print(getFieldName());
    printStream.print(';');
    if (isIdentityField()) {
      printStream.print(" // identity");
    }
    if (isDeleted()) {
      printStream.print(" // DELETED");
    }

    printStream.println();
  }
}
