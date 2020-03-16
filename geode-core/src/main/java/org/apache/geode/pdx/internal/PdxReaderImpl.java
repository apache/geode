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
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.tcp.ByteBufferInputStream;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxFieldTypeMismatchException;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxUnreadFields;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;
import org.apache.geode.pdx.internal.AutoSerializableManager.PdxFieldWrapper;

/**
 * A new instance of this class is created each time we deserialize a pdx. It is also used as the
 * base class of our {@link PdxInstance} implementation. It is serializable because PdxInstance is.
 *
 * @since GemFire 6.6
 */
public class PdxReaderImpl implements InternalPdxReader, java.io.Serializable {

  private static final long serialVersionUID = -6094553093860427759L;
  /**
   * This is the type the blob we are reading was encoded with.
   */
  private final PdxType blobType;
  private final PdxInputStream dis;
  private transient PdxUnreadData readUnreadFieldsCalled;

  protected PdxReaderImpl(PdxReaderImpl copy) {
    this.blobType = copy.blobType;
    this.dis = new PdxInputStream(copy.dis);
    this.readUnreadFieldsCalled = copy.getReadUnreadFieldsCalled();
  }

  public PdxReaderImpl(PdxType pdxType, DataInput in, int len) throws IOException {
    this(pdxType, createDis(in, len));
  }

  private static PdxInputStream createDis(DataInput in, int len) throws IOException {
    boolean isBBIS = in instanceof ByteBufferInputStream;
    PdxInputStream bbis;
    if (isBBIS) {
      // Note, it is ok for our immutable bbis to wrap a mutable bbis
      // because PdxReaderImpl is only used for a quick deserialization.
      // PdxInstanceImpl has its own flavor of createDis since it can
      // live longer.
      bbis = new PdxInputStream((ByteBufferInputStream) in, len);
      int bytesSkipped = in.skipBytes(len);
      int bytesRemaining = len - bytesSkipped;
      while (bytesRemaining > 0) {
        in.readByte();
        bytesRemaining--;
      }
    } else {
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      bbis = new PdxInputStream(bytes);
    }
    return bbis;
  }

  protected PdxReaderImpl(PdxType pdxType, PdxInputStream bbis) {
    this.blobType = pdxType;
    this.dis = bbis;
  }

  public static final int MAX_UNSIGNED_BYTE = 255;
  public static final int MAX_UNSIGNED_SHORT = 65535;

  /**
   * @return size of each variable length field offset
   */
  private byte getSizeOfOffset() {
    int size = this.dis.size();
    if (size <= MAX_UNSIGNED_BYTE) {
      return DataSize.BYTE_SIZE;
    } else if (size <= MAX_UNSIGNED_SHORT) {
      return DataSize.SHORT_SIZE;
    }
    return DataSize.INTEGER_SIZE;
  }

  @Override
  public PdxField getPdxField(String fieldName) {
    return this.blobType.getPdxField(fieldName);
  }

  @Override
  public char readChar(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.CHAR) {
      throw new PdxFieldTypeMismatchException(
          "Expected char field but found field of type " + ft.getTypeIdString());
    }
    return readChar(ft);
  }

  @Override
  public char readChar(PdxField ft) {
    return dis.readChar(getPositionForField(ft));
  }

  @Override
  public char readChar() {
    return dis.readChar();
  }

  @Override
  public boolean readBoolean(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return false;
    }
    if (ft.getFieldType() != FieldType.BOOLEAN) {
      throw new PdxFieldTypeMismatchException(
          "Expected boolean field but found field of type " + ft.getTypeIdString());
    }
    return readBoolean(ft);
  }

  @Override
  public boolean readBoolean(PdxField ft) {
    return dis.readBoolean(getPositionForField(ft));
  }

  @Override
  public boolean readBoolean() {
    return dis.readBoolean();
  }

  @Override
  public byte readByte(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.BYTE) {
      throw new PdxFieldTypeMismatchException(
          "Expected byte field but found field of type " + ft.getTypeIdString());
    }
    return readByte(ft);
  }

  @Override
  public byte readByte(PdxField ft) {
    return dis.readByte(getPositionForField(ft));
  }

  @Override
  public byte readByte() {
    return dis.readByte();
  }

  @Override
  public short readShort(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.SHORT) {
      throw new PdxFieldTypeMismatchException(
          "Expected short field but found field of type " + ft.getTypeIdString());
    }
    return readShort(ft);
  }

  @Override
  public short readShort(PdxField ft) {
    return dis.readShort(getPositionForField(ft));
  }

  @Override
  public short readShort() {
    return dis.readShort();
  }

  @Override
  public int readInt(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.INT) {
      throw new PdxFieldTypeMismatchException(
          "Expected int field but found field of type " + ft.getTypeIdString());
    }
    return readInt(ft);
  }

  @Override
  public int readInt(PdxField ft) {
    return dis.readInt(getPositionForField(ft));
  }

  @Override
  public int readInt() {
    return dis.readInt();
  }

  @Override
  public long readLong(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.LONG) {
      throw new PdxFieldTypeMismatchException(
          "Expected long field but found field of type " + ft.getTypeIdString());
    }
    return readLong(ft);
  }

  @Override
  public long readLong(PdxField ft) {
    return dis.readLong(getPositionForField(ft));
  }

  @Override
  public long readLong() {
    return dis.readLong();
  }

  @Override
  public float readFloat(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.FLOAT) {
      throw new PdxFieldTypeMismatchException(
          "Expected float field but found field of type " + ft.getTypeIdString());
    }
    return readFloat(ft);
  }

  @Override
  public float readFloat(PdxField ft) {
    return dis.readFloat(getPositionForField(ft));
  }

  @Override
  public float readFloat() {
    return dis.readFloat();
  }

  @Override
  public double readDouble(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return 0;
    }
    if (ft.getFieldType() != FieldType.DOUBLE) {
      throw new PdxFieldTypeMismatchException(
          "Expected double field but found field of type " + ft.getTypeIdString());
    }
    return readDouble(ft);
  }

  @Override
  public double readDouble(PdxField ft) {
    return dis.readDouble(getPositionForField(ft));
  }

  @Override
  public double readDouble() {
    return dis.readDouble();
  }

  @Override
  public Date readDate(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.DATE) {
      throw new PdxFieldTypeMismatchException(
          "Expected Date field but found field of type " + ft.getTypeIdString());
    }
    return readDate(ft);
  }

  @Override
  public Date readDate(PdxField ft) {
    return this.dis.readDate(getPositionForField(ft));
  }

  @Override
  public Date readDate() {
    return this.dis.readDate();
  }

  @Override
  public String readString(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.STRING) {
      throw new PdxFieldTypeMismatchException(
          "Expected String field but found field of type " + ft.getTypeIdString());
    }
    return readString(ft);
  }

  @Override
  public String readString(PdxField ft) {
    return this.dis.readString(getPositionForField(ft));
  }

  @Override
  public String readString() {
    return this.dis.readString();
  }

  @Override
  public Object readObject(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.OBJECT) {
      throw new PdxFieldTypeMismatchException(
          "Expected Object field but found field of type " + ft.getTypeIdString());
    }
    return readObject(ft);
  }

  @Override
  public Object readObject(PdxField ft) {
    if (ft instanceof DefaultPdxField) {
      return null; // default object value
    }
    return this.dis.readObject(getPositionForField(ft));
  }

  @Override
  public Object readObject() {
    return this.dis.readObject();
  }

  @Override
  public char[] readCharArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.CHAR_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected char[] field but found field of type " + ft.getTypeIdString());
    }
    return readCharArray(ft);
  }

  @Override
  public char[] readCharArray(PdxField ft) {
    return this.dis.readCharArray(getPositionForField(ft));
  }

  @Override
  public char[] readCharArray() {
    return this.dis.readCharArray();
  }

  @Override
  public boolean[] readBooleanArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.BOOLEAN_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected boolean[] field but found field of type " + ft.getTypeIdString());
    }
    return readBooleanArray(ft);
  }

  @Override
  public boolean[] readBooleanArray(PdxField ft) {
    return this.dis.readBooleanArray(getPositionForField(ft));
  }

  @Override
  public boolean[] readBooleanArray() {
    return this.dis.readBooleanArray();
  }

  @Override
  public byte[] readByteArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.BYTE_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected byte[] field but found field of type " + ft.getTypeIdString());
    }
    return readByteArray(ft);
  }

  @Override
  public byte[] readByteArray(PdxField ft) {
    return this.dis.readByteArray(getPositionForField(ft));
  }

  @Override
  public byte[] readByteArray() {
    return this.dis.readByteArray();
  }

  @Override
  public short[] readShortArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.SHORT_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected short[] field but found field of type " + ft.getTypeIdString());
    }
    return readShortArray(ft);
  }

  @Override
  public short[] readShortArray(PdxField ft) {
    return this.dis.readShortArray(getPositionForField(ft));
  }

  @Override
  public short[] readShortArray() {
    return this.dis.readShortArray();
  }

  @Override
  public int[] readIntArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.INT_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected int[] field but found field of type " + ft.getTypeIdString());
    }
    return readIntArray(ft);
  }

  @Override
  public int[] readIntArray(PdxField ft) {
    return this.dis.readIntArray(getPositionForField(ft));
  }

  @Override
  public int[] readIntArray() {
    return this.dis.readIntArray();
  }

  @Override
  public long[] readLongArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.LONG_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected long[] field but found field of type " + ft.getTypeIdString());
    }
    return readLongArray(ft);
  }

  @Override
  public long[] readLongArray(PdxField ft) {
    return this.dis.readLongArray(getPositionForField(ft));
  }

  @Override
  public long[] readLongArray() {
    return this.dis.readLongArray();
  }

  @Override
  public float[] readFloatArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.FLOAT_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected float[] field but found field of type " + ft.getTypeIdString());
    }
    return readFloatArray(ft);
  }

  @Override
  public float[] readFloatArray(PdxField ft) {
    return this.dis.readFloatArray(getPositionForField(ft));
  }

  @Override
  public float[] readFloatArray() {
    return this.dis.readFloatArray();
  }

  @Override
  public double[] readDoubleArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.DOUBLE_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected double[] field but found field of type " + ft.getTypeIdString());
    }
    return readDoubleArray(ft);
  }

  @Override
  public double[] readDoubleArray(PdxField ft) {
    return this.dis.readDoubleArray(getPositionForField(ft));
  }

  @Override
  public double[] readDoubleArray() {
    return this.dis.readDoubleArray();
  }

  @Override
  public String[] readStringArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.STRING_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected String[] field but found field of type " + ft.getTypeIdString());
    }
    return readStringArray(ft);
  }

  @Override
  public String[] readStringArray(PdxField ft) {
    return this.dis.readStringArray(getPositionForField(ft));
  }

  @Override
  public String[] readStringArray() {
    return this.dis.readStringArray();
  }

  @Override
  public Object[] readObjectArray(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.OBJECT_ARRAY) {
      throw new PdxFieldTypeMismatchException(
          "Expected Object[] field but found field of type " + ft.getTypeIdString());
    }
    return readObjectArray(ft);
  }

  @Override
  public Object[] readObjectArray(PdxField ft) {
    if (ft instanceof DefaultPdxField) {
      return null; // default array value
    }
    return this.dis.readObjectArray(getPositionForField(ft));
  }

  @Override
  public Object[] readObjectArray() {
    return this.dis.readObjectArray();
  }

  @Override
  public byte[][] readArrayOfByteArrays(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() != FieldType.ARRAY_OF_BYTE_ARRAYS) {
      throw new PdxFieldTypeMismatchException(
          "Expected byte[][] field but found field of type " + ft.getTypeIdString());
    }
    return readArrayOfByteArrays(ft);
  }

  @Override
  public byte[][] readArrayOfByteArrays(PdxField ft) {
    return this.dis.readArrayOfByteArrays(getPositionForField(ft));
  }

  @Override
  public byte[][] readArrayOfByteArrays() {
    return this.dis.readArrayOfByteArrays();
  }

  /**
   * @param idx of the variable length field
   * @return the offset to the variable length field
   */
  private int getOffset(int idx) {
    int size = this.dis.size();
    if (size <= MAX_UNSIGNED_BYTE) {
      return dis.readByte(size - idx * DataSize.BYTE_SIZE) & MAX_UNSIGNED_BYTE;
    } else if (size <= MAX_UNSIGNED_SHORT) {
      return dis.readShort(size - idx * DataSize.SHORT_SIZE) & MAX_UNSIGNED_SHORT;
    } else {
      return dis.readInt(size - idx * DataSize.INTEGER_SIZE);
    }
  }

  private int getPositionForField(PdxField ft) {
    return getAbsolutePosition(ft);
  }

  private int getAbsolutePosition(PdxField ft) {
    int pos = 0;
    int idx0 = ft.getRelativeOffset();
    int idx1 = ft.getVlfOffsetIndex();

    if (ft.isVariableLengthType()) {
      if (idx1 != -1) {
        pos = getOffset(idx1);
      } else {
        pos = idx0;
      }
    } else {
      if (idx0 >= 0) {
        // index0 indicates the offset
        pos = idx0;
      } else if (idx1 > 0) {
        // index1 indicates the offset no. Read the value at that offset
        // then move backward index0 positions backward from that value.
        pos = getOffset(idx1) + idx0;
      } else if (idx1 == -1) {
        // index0 indicates no position backward from last offset position
        pos = getOffsetToVlfTable() + idx0;
      } else {
        throw new InternalGemFireException("idx0=" + idx0 + " idx1=" + idx1);
      }
    }
    return pos;
  }

  private int getOffsetToVlfTable() {
    return this.dis.size() - blobType.getVariableLengthFieldCount() * getSizeOfOffset();
  }

  @Override
  public boolean hasField(String fieldName) {
    return blobType.getPdxField(fieldName) != null;
  }

  @Override
  public boolean isIdentityField(String fieldName) {
    PdxField field = blobType.getPdxField(fieldName);
    return field != null && field.isIdentityField();
  }

  @Override
  public Object readField(String fieldName) {
    PdxField ft = blobType.getPdxField(fieldName);
    if (ft == null) {
      return null;
    }
    switch (ft.getFieldType()) {
      case CHAR:
        return readChar(ft);
      case BOOLEAN:
        return readBoolean(ft);
      case BYTE:
        return readByte(ft);
      case SHORT:
        return readShort(ft);
      case INT:
        return readInt(ft);
      case LONG:
        return readLong(ft);
      case FLOAT:
        return readFloat(ft);
      case DOUBLE:
        return readDouble(ft);
      case DATE:
        return readDate(ft);
      case STRING:
        return readString(ft);
      case OBJECT:
        return readObject(ft);
      case BOOLEAN_ARRAY:
        return readBooleanArray(ft);
      case CHAR_ARRAY:
        return readCharArray(ft);
      case BYTE_ARRAY:
        return readByteArray(ft);
      case SHORT_ARRAY:
        return readShortArray(ft);
      case INT_ARRAY:
        return readIntArray(ft);
      case LONG_ARRAY:
        return readLongArray(ft);
      case FLOAT_ARRAY:
        return readFloatArray(ft);
      case DOUBLE_ARRAY:
        return readDoubleArray(ft);
      case STRING_ARRAY:
        return readStringArray(ft);
      case OBJECT_ARRAY:
        return readObjectArray(ft);
      case ARRAY_OF_BYTE_ARRAYS:
        return readArrayOfByteArrays(ft);
      default:
        throw new InternalGemFireException("Unhandled field type " + ft.getFieldType());
    }
  }

  @MutableForTesting
  public static boolean TESTHOOK_TRACKREADS = false;

  public Object getObject() throws IOException, ClassNotFoundException {
    return basicGetObject();
  }

  protected Object basicGetObject() {
    String pdxClassName = getPdxType().getClassName();
    Class<?> pdxClass;
    try {
      pdxClass = InternalDataSerializer.getCachedClass(pdxClassName);
    } catch (Exception e) {
      PdxSerializationException ex = new PdxSerializationException(
          String.format("Could not create an instance of a class %s",
              pdxClassName),
          e);
      throw ex;
    }
    {
      AutoClassInfo ci = getPdxType().getAutoInfo(pdxClass);
      if (ci != null) {
        Object obj = ci.newInstance(pdxClass);
        this.orderedDeserialize(obj, ci);
        return obj;
      }
    }
    PdxReader pdxReader = this;
    // only create a tracking one if we might need it
    UnreadPdxType unreadLocalPdxType = null;
    boolean needToTrackReads = TESTHOOK_TRACKREADS;
    InternalCache cache = GemFireCacheImpl
        .getForPdx("PDX registry is unavailable because the Cache has been closed.");
    TypeRegistry tr = cache.getPdxRegistry();
    if (!cache.getPdxIgnoreUnreadFields()) {
      PdxType localPdxType = tr.getExistingTypeForClass(pdxClass);
      if (localPdxType != null) {
        if (getPdxType().getTypeId() != localPdxType.getTypeId()
            && getPdxType().hasExtraFields(localPdxType)) {
          // we could calculate the extra fields here
          needToTrackReads = true;
        }
      } else {
        // we don't know what our local type would be
        needToTrackReads = true;
      }
    }

    if (needToTrackReads) {
      unreadLocalPdxType = tr.getExistingTypeForClass(pdxClass, getPdxType().getTypeId());
      if (unreadLocalPdxType != null) {
        needToTrackReads = false;
      } else {
        pdxReader = new TrackingPdxReaderImpl(this, tr, pdxClass);
      }
    }

    Object result;
    if (PdxSerializable.class.isAssignableFrom(pdxClass)) {
      try {
        result = pdxClass.newInstance();
      } catch (Exception e) {
        PdxSerializationException ex = new PdxSerializationException(
            String.format("Could not create an instance of a class %s",
                pdxClassName),
            e);
        throw ex;
      }
      ((PdxSerializable) result).fromData(pdxReader);
    } else {
      PdxSerializer pdxSerializer = cache.getPdxSerializer();
      if (pdxSerializer != null) {
        result = pdxSerializer.fromData(pdxClass, pdxReader);
        if (result == null) {
          throw new PdxSerializationException(
              "Could not deserialize pdx because the pdx serializer's fromData returned false for a pdx of class "
                  + pdxClassName);
        }
      } else {
        throw new PdxSerializationException(
            "Could not deserialize pdx because a PdxSerializer does not exist.");
      }
    }
    {
      PdxUnreadData ud = getReadUnreadFieldsCalled();
      if (ud != null) {
        // User called PdxReader.readUnreadFields()
        if (unreadLocalPdxType != null) {
          if (unreadLocalPdxType.getUnreadFieldIndexes() != null) {
            ud.initialize(unreadLocalPdxType, this);
          }
        } else if (needToTrackReads) {
          ((TrackingPdxReaderImpl) pdxReader).internalReadUnreadFields(ud);
        }
      } else {
        if (needToTrackReads) {
          ud = ((TrackingPdxReaderImpl) pdxReader).internalReadUnreadFields(new PdxUnreadData());
          if (ud != null && !ud.isEmpty()) {
            tr.putUnreadData(result, ud);
          }
        } else if (unreadLocalPdxType != null) {
          if (unreadLocalPdxType.getUnreadFieldIndexes() != null) {
            tr.putUnreadData(result, new PdxUnreadData(unreadLocalPdxType, this));
          }
        }
      }
    }
    return result;
  }

  PdxUnreadData getReadUnreadFieldsCalled() {
    return this.readUnreadFieldsCalled;
  }

  void setReadUnreadFieldsCalled(PdxUnreadData v) {
    this.readUnreadFieldsCalled = v;
  }

  @Override
  public PdxType getPdxType() {
    return this.blobType;
  }

  public ByteSource getRaw(int fieldIdx) {
    PdxField ft = getPdxType().getPdxFieldByIndex(fieldIdx);
    if (ft == null) {
      throw new InternalGemFireException("unknown field " + fieldIdx);
    }
    return getRaw(ft);
  }

  protected ByteSource getRaw(PdxField ft) {
    if (ft instanceof DefaultPdxField) {
      return ((DefaultPdxField) ft).getDefaultBytes();
    }
    int startOffset = getAbsolutePosition(ft);
    int nextFieldIdx = ft.getFieldIndex() + 1;
    int endOffset;
    if (nextFieldIdx >= getPdxType().getFieldCount()) {
      endOffset = getOffsetToVlfTable();
    } else {
      endOffset = getAbsolutePosition(getPdxType().getPdxFieldByIndex(nextFieldIdx));
    }
    return this.dis.slice(startOffset, endOffset);
  }

  @Override
  public PdxUnreadFields readUnreadFields() {
    PdxUnreadData result = new PdxUnreadData();
    setReadUnreadFieldsCalled(result);
    return result;
  }

  protected void basicSendTo(DataOutput out) throws IOException {
    this.dis.sendTo(out);
  }

  protected void basicSendTo(ByteBuffer bb) {
    this.dis.sendTo(bb);
  }

  protected int basicSize() {
    return this.dis.size();
  }

  protected void basicSetBuffer(ByteBuffer bb) {
    this.dis.setBuffer(bb);
  }

  /**
   * Provides optimized deserialization when a blob exactly matches the fields of the target class
   * when using {@link AutoSerializableManager}.
   *
   * @param obj the target object we are deserializing into
   * @param ci the meta information generated by the auto serializer
   */
  @Override
  public void orderedDeserialize(Object obj, AutoClassInfo ci) {
    PdxReaderImpl reader = prepForOrderedReading();
    for (PdxFieldWrapper f : ci.getFields()) {
      f.orderedDeserialize(reader, obj);
    }
  }

  /**
   * Return a reader that can do ordered reading.
   */
  private PdxReaderImpl prepForOrderedReading() {
    PdxReaderImpl result = this;
    if (this.dis instanceof PdxInputStream) {
      result = new PdxReaderImpl(this);
    }
    int pos = 0;
    if (result.blobType.getFieldCount() > 0) {
      pos = getPositionForField(result.blobType.getFields().get(0));
    }
    result.dis.position(pos);
    return result;
  }

  /**
   * @return PdxString if field is a String otherwise invokes {@link #readField(String)}
   */
  public Object readRawField(String field) {
    PdxField ft = blobType.getPdxField(field);
    if (ft == null) {
      return null;
    }
    if (ft.getFieldType() == FieldType.STRING) {
      return readPdxString(ft);
    } else {
      PdxString pdxString = getPdxStringFromObjectField(ft);
      if (pdxString != null)
        return pdxString;
    }
    return readField(field);
  }

  /**
   * This method checks whether Object field is String type. If its String then it returns PdxString
   * otherwise null.
   */
  private PdxString getPdxStringFromObjectField(PdxField ft) {
    if (ft.getFieldType() == FieldType.OBJECT) {
      ByteSource buffer = dis.getBuffer();
      byte[] bytes = null;
      if (buffer.hasArray()) {
        bytes = buffer.array();
      } else {
        throw new IllegalStateException();
      }
      int offset = getPositionForField(ft) + buffer.arrayOffset();
      // Do not create PdxString if the field is NULL
      if (bytes[offset] == DSCODE.STRING.toByte() || bytes[offset] == DSCODE.STRING_BYTES.toByte()
          || bytes[offset] == DSCODE.HUGE_STRING.toByte()
          || bytes[offset] == DSCODE.HUGE_STRING_BYTES.toByte()) {
        return new PdxString(bytes, offset);
      }
    }
    return null;
  }

  /**
   * @return returns {@link PdxString}
   */
  public PdxString readPdxString(PdxField ft) {
    ByteSource buffer = dis.getBuffer();
    byte[] bytes = null;
    if (buffer.hasArray()) {
      bytes = buffer.array();
    } else {
      throw new IllegalStateException();
    }
    int offset = getPositionForField(ft) + buffer.arrayOffset();
    // Do not create PdxString if the field is NULL
    if (bytes[offset] == DSCODE.NULL.toByte() || bytes[offset] == DSCODE.NULL_STRING.toByte()) {
      return null;
    }
    return new PdxString(bytes, offset);
  }
}
