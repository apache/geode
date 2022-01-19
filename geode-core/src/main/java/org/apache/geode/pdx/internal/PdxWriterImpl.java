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
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxFieldAlreadyExistsException;
import org.apache.geode.pdx.PdxFieldDoesNotExistException;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxUnreadFields;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A new instance of this class is created for each (nested) instance of {@link PdxSerializable}.
 * But it may share the underlying instance of {@link HeapDataOutputStream} with other instances of
 * this class.
 *
 * @since GemFire 6.6
 * @see InternalDataSerializer#basicWriteObject(Object, java.io.DataOutput, boolean)
 */
public class PdxWriterImpl implements PdxWriter {

  public static final byte TYPE_ID_SIZE = DataSize.INTEGER_SIZE;
  public static final byte HEADER_SIZE = TYPE_ID_SIZE + DataSize.INTEGER_SIZE + DataSize.BYTE_SIZE;
  public static final int EXPAND_SIZE = 32; // used for number of offsets array

  /**
   * tr is no longer final because it is initialized late when using a PdxSerializer.
   */
  private TypeRegistry tr;

  private final Object pdx;
  private final PdxOutputStream os;
  private final AutoClassInfo aci;

  /**
   * Offsets to the variable length fields.
   */
  private int[] vlfOffsets;

  /**
   * The number of variable length fields that need an offset. The first VLF does not need an
   * offset.
   */
  private int vlfCount = 0;

  private boolean hasSeenFirstVlf = false;

  /**
   * The offset into the hdos to the header.
   */
  protected final int headerOffset;

  private PdxUnreadData unreadData;

  private PdxType existingType;
  private PdxType newType;
  private int fieldId = -1;

  /**
   * If true then extra validation is done to detect if mistakes have been made in the way PdxWriter
   * is used. Currently this will cause PdxSerializationException to be thrown if the number, names,
   * or types of fields are changed or if different identity fields are marked on the same instance
   * of a class. This property should only be set when debugging new code since it will slow down
   * pdx serialization.
   */
  private static final boolean sysPropDoExtraPdxValidation =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "validatePdxWriters");

  private boolean doExtraValidation = sysPropDoExtraPdxValidation;

  public PdxWriterImpl(TypeRegistry tr, Object pdx, PdxOutputStream out) {
    this.tr = tr;
    this.pdx = pdx;
    os = out;
    headerOffset = os.size();
    aci = null;
  }

  PdxWriterImpl(PdxType pdxType, PdxOutputStream out) {
    tr = null;
    pdx = null;
    os = out;
    existingType = pdxType;
    headerOffset = os.size();
    aci = null;
  }

  PdxWriterImpl(PdxType pt, TypeRegistry tr, PdxOutputStream out) {
    this.tr = tr;
    pdx = null;
    os = out;
    newType = pt;
    headerOffset = os.size();
    aci = null;
  }

  public PdxWriterImpl(TypeRegistry tr, Object pdx, AutoClassInfo aci, PdxOutputStream os) {
    this.tr = tr;
    this.pdx = pdx;
    this.os = os;
    headerOffset = this.os.size();
    this.aci = aci;
  }

  private boolean fieldsWritten() {
    return fieldId >= 0;
  }

  private void beforeFieldWrite() {
    ++fieldId;
    if (fieldId > 0) {
      // already wrote first field
      return;
    }
    initialize();
  }

  private void initialize() {
    writeHeader();
    if (existingType != null) {
      // PdxInstance is using us to flush its dirty fields
      return;
    }
    if (definingNewPdxType()) {
      // PdxInstanceFactoryImpl is using us
      return;
    }
    PdxUnreadData ud = initUnreadData();
    if (ud == null && pdx != null) {
      if (aci != null) {
        existingType = aci.getSerializedType();
      } else {
        existingType = tr.getExistingType(pdx);
      }
    } else if (ud != null) {
      existingType = ud.getSerializedType();
    }

    if (existingType != null) {
      int c = existingType.getVariableLengthFieldCount();
      if (c > 0) {
        vlfOffsets = new int[c];
      }
    } else if (pdx != null) {
      newType = new PdxType(pdx.getClass().getName(), true);
    }
  }

  private boolean unreadDataInitialized = false;

  PdxUnreadData initUnreadData() {
    if (unreadDataInitialized) {
      return unreadData;
    }
    unreadDataInitialized = true;
    if (tr == null) {
      // We are being PdxSerializer serialized.
      // Now is the time to initialize tr.
      tr = GemFireCacheImpl.getForPdx("Could not access Pdx registry").getPdxRegistry();
    }
    PdxUnreadData ud = unreadData;
    if (ud == null && pdx != null) {
      ud = tr.getUnreadData(pdx);
      unreadData = ud;
    }
    return ud;
  }

  @Override
  public PdxWriter writeChar(String fieldName, char value) {
    updateMetaData(fieldName, FieldType.CHAR);
    os.writeChar(value);
    return this;
  }

  public void writeChar(char value) {
    beforeFieldWrite();
    os.writeChar(value);
  }

  @Override
  public PdxWriter writeBoolean(String fieldName, boolean value) {
    updateMetaData(fieldName, FieldType.BOOLEAN);
    os.writeByte((value) ? 0x1 : 0x0);
    return this;
  }

  public void writeBoolean(boolean value) {
    beforeFieldWrite();
    os.writeByte((value) ? 0x1 : 0x0);
  }

  @Override
  public PdxWriter writeByte(String fieldName, byte value) {
    updateMetaData(fieldName, FieldType.BYTE);
    os.writeByte(value);
    return this;
  }

  public void writeByte(byte value) {
    beforeFieldWrite();
    os.writeByte(value);
  }

  @Override
  public PdxWriter writeShort(String fieldName, short value) {
    updateMetaData(fieldName, FieldType.SHORT);
    os.writeShort(value);
    return this;
  }

  public void writeShort(short value) {
    beforeFieldWrite();
    os.writeShort(value);
  }

  @Override
  public PdxWriter writeInt(String fieldName, int value) {
    updateMetaData(fieldName, FieldType.INT);
    os.writeInt(value);
    return this;
  }

  public void writeInt(int value) {
    beforeFieldWrite();
    os.writeInt(value);
  }

  @Override
  public PdxWriter writeLong(String fieldName, long value) {
    updateMetaData(fieldName, FieldType.LONG);
    os.writeLong(value);
    return this;
  }

  public void writeLong(long value) {
    beforeFieldWrite();
    os.writeLong(value);
  }

  @Override
  public PdxWriter writeFloat(String fieldName, float value) {
    updateMetaData(fieldName, FieldType.FLOAT);
    os.writeFloat(value);
    return this;
  }

  public void writeFloat(float value) {
    beforeFieldWrite();
    os.writeFloat(value);
  }

  @Override
  public PdxWriter writeDouble(String fieldName, double value) {
    updateMetaData(fieldName, FieldType.DOUBLE);
    os.writeDouble(value);
    return this;
  }

  public void writeDouble(double value) {
    beforeFieldWrite();
    os.writeDouble(value);
  }

  @Override
  public PdxWriter writeDate(String fieldName, Date date) {
    if (date != null && !Date.class.equals(date.getClass())) {
      // fix for bug 43717
      throw new IllegalArgumentException(
          "writeDate only accepts instances of Date. Subclasses are not supported. Use writeObject for subclasses of Date.");
    }
    updateMetaData(fieldName, FieldType.DATE);
    os.writeDate(date);
    return this;
  }

  public void writeDate(Date date) {
    if (date != null && !Date.class.equals(date.getClass())) {
      // fix for bug 43717
      throw new IllegalArgumentException(
          "writeDate only accepts instances of Date. Subclasses are not supported. Use writeObject for subclasses of Date.");
    }
    beforeFieldWrite();
    os.writeDate(date);
  }

  @Override
  public PdxWriter writeString(String fieldName, String value) {
    markVariableField();
    updateMetaData(fieldName, FieldType.STRING);
    os.writeString(value);
    return this;
  }

  public void writeString(String value) {
    markVariableField();
    beforeFieldWrite();
    os.writeString(value);
  }

  @Override
  public PdxWriter writeObject(String fieldName, Object object) {
    return writeObject(fieldName, object, false);
  }

  public void writeObject(Object object) {
    writeObject(object, false);
  }

  @Override
  public PdxWriter writeObject(String fieldName, Object object, boolean onlyPortableObjects) {
    markVariableField();
    updateMetaData(fieldName, FieldType.OBJECT);
    os.writeObject(object, onlyPortableObjects);
    return this;
  }

  public void writeObject(Object object, boolean onlyPortableObjects) {
    markVariableField();
    beforeFieldWrite();
    os.writeObject(object, onlyPortableObjects);
  }

  @Override
  public PdxWriter writeBooleanArray(String fieldName, boolean[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.BOOLEAN_ARRAY);
    os.writeBooleanArray(array);
    return this;
  }

  public void writeBooleanArray(boolean[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeBooleanArray(array);
  }

  @Override
  public PdxWriter writeCharArray(String fieldName, char[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.CHAR_ARRAY);
    os.writeCharArray(array);
    return this;
  }

  public void writeCharArray(char[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeCharArray(array);
  }

  @Override
  public PdxWriter writeByteArray(String fieldName, byte[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.BYTE_ARRAY);
    os.writeByteArray(array);
    return this;
  }

  public void writeByteArray(byte[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeByteArray(array);
  }

  @Override
  public PdxWriter writeShortArray(String fieldName, short[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.SHORT_ARRAY);
    os.writeShortArray(array);
    return this;
  }

  public void writeShortArray(short[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeShortArray(array);
  }

  @Override
  public PdxWriter writeIntArray(String fieldName, int[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.INT_ARRAY);
    os.writeIntArray(array);
    return this;
  }

  public void writeIntArray(int[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeIntArray(array);
  }

  @Override
  public PdxWriter writeLongArray(String fieldName, long[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.LONG_ARRAY);
    os.writeLongArray(array);
    return this;
  }

  public void writeLongArray(long[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeLongArray(array);
  }

  @Override
  public PdxWriter writeFloatArray(String fieldName, float[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.FLOAT_ARRAY);
    os.writeFloatArray(array);
    return this;
  }

  public void writeFloatArray(float[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeFloatArray(array);
  }

  @Override
  public PdxWriter writeDoubleArray(String fieldName, double[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.DOUBLE_ARRAY);
    os.writeDoubleArray(array);
    return this;
  }

  public void writeDoubleArray(double[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeDoubleArray(array);
  }

  @Override
  public PdxWriter writeStringArray(String fieldName, String[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.STRING_ARRAY);
    os.writeStringArray(array);
    return this;
  }

  public void writeStringArray(String[] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeStringArray(array);
  }

  @Override
  public PdxWriter writeObjectArray(String fieldName, Object[] array) {
    return writeObjectArray(fieldName, array, false);
  }

  public void writeObjectArray(Object[] array) {
    writeObjectArray(array, false);
  }

  @Override
  public PdxWriter writeObjectArray(String fieldName, Object[] array, boolean onlyPortableObjects) {
    markVariableField();
    updateMetaData(fieldName, FieldType.OBJECT_ARRAY);
    os.writeObjectArray(array, onlyPortableObjects);
    return this;
  }

  public void writeObjectArray(Object[] array, boolean onlyPortableObjects) {
    markVariableField();
    beforeFieldWrite();
    os.writeObjectArray(array, onlyPortableObjects);
  }

  @Override
  public PdxWriter writeArrayOfByteArrays(String fieldName, byte[][] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.ARRAY_OF_BYTE_ARRAYS);
    os.writeArrayOfByteArrays(array);
    return this;
  }

  public void writeArrayOfByteArrays(byte[][] array) {
    markVariableField();
    beforeFieldWrite();
    os.writeArrayOfByteArrays(array);
  }

  private boolean alreadyGenerated = false;

  /**
   * Must be invoked only after {@link PdxSerializable#toData(PdxWriter)}
   *
   * @return total number of bytes serialized for this pdx
   */
  public int completeByteStreamGeneration() {
    if (!alreadyGenerated) {
      alreadyGenerated = true;
      if (!fieldsWritten()) {
        initialize();
      }
      writeUnreadData();
      appendOffsets();
      int typeId;
      if (definingNewPdxType()) {
        newType.initialize(this);
        if (unreadData != null && !unreadData.isEmpty()) {
          // We created a new type that had unreadData.
          // In this case we don't define a local type
          // but we do set the serialized type.
          newType = tr.defineType(newType);
          typeId = newType.getTypeId();
          unreadData.setSerializedType(newType);
        } else {
          newType = tr.defineLocalType(pdx, newType);
          typeId = newType.getTypeId();
        }
      } else {
        if (doExtraValidation()) {
          int fieldCount = fieldId + 1;
          if (existingType.getFieldCount() != fieldCount) {
            throw new PdxSerializationException("Expected the number of fields for class "
                + existingType.getClassName() + " to be " + existingType.getFieldCount()
                + " but instead it was " + fieldCount);
          }
        }
        typeId = existingType.getTypeId();
      }

      // Now write length of the byte stream (does not include bytes for DSCODE and the length
      // itself.)
      long bits = ((long) getCurrentOffset()) << 32 | (0x00000000FFFFFFFFL & typeId); // fixes 45005
      lu.update(bits);
    } // !alreadyGenerated

    return getCurrentOffset() + 1; // +1 for DSCODE.PDX.toByte()
  }

  /**
   * Returns the pdx type that can be used by the auto serializer to always serialize this class.
   */
  public PdxType getAutoPdxType() {
    if (unreadData != null && !unreadData.isEmpty()) {
      return null;
    }
    completeByteStreamGeneration();
    if (definingNewPdxType()) {
      return newType;
    } else {
      return existingType;
    }
  }

  public PdxType getPdxType() {
    return newType;
  }

  /**
   * @return the offset to the byte of the first field
   */
  private int getBaseOffset() {
    return headerOffset + DataSize.BYTE_SIZE + (DataSize.INTEGER_SIZE * 2);
  }

  private int getCurrentOffset() {
    return os.size() - getBaseOffset();
  }

  /**
   * Must be invoked only after {@link PdxSerializable#toData(PdxWriter)}
   */
  private void appendOffsets() {
    int fieldDataSize = getCurrentOffset();
    // Take the list of offsets and append it in reverse order.
    byte sizeOfOffset = getSizeOfOffset(vlfCount, fieldDataSize);
    for (int i = (vlfCount - 1); i >= 0; i--) {
      switch (sizeOfOffset) {
        case 1:
          os.write((byte) vlfOffsets[i]);
          break;
        case 2:
          os.writeShort((short) vlfOffsets[i]);
          break;
        case 4:
          os.writeInt(vlfOffsets[i]);
          break;
        default:
          break;
      }
    }
  }

  /**
   * This is required while writing the byte stream.
   *
   * @param offsetCount Number of offsets to appended in this byte stream.
   * @param size Size of the byte stream (excluding DSCODE, length int and the offsets.)
   * @return size of each offset
   */
  public static byte getSizeOfOffset(int offsetCount, int size) {
    if (offsetCount < 0 || size < 0) {
      throw new InternalGemFireException(
          "Values cannot be negative. offsetCount: " + offsetCount + ", size: " + size + " bytes");
    }

    if (((offsetCount * DataSize.BYTE_SIZE) + size) <= PdxReaderImpl.MAX_UNSIGNED_BYTE) {
      return DataSize.BYTE_SIZE;
    } else if (((offsetCount * DataSize.SHORT_SIZE) + size) <= PdxReaderImpl.MAX_UNSIGNED_SHORT) {
      return DataSize.SHORT_SIZE;
    } else {
      return DataSize.INTEGER_SIZE;
    }
  }

  public void sendTo(DataOutput out) throws IOException {
    os.sendTo(out);
  }

  public byte[] toByteArray() {
    return os.toByteArray();
  }

  private void markVariableField() {
    if (!hasSeenFirstVlf) {
      hasSeenFirstVlf = true;
    } else {
      ensureVlfCapacity();
      vlfOffsets[vlfCount] = getCurrentOffset();
      vlfCount++;
    }
  }

  /**
   * Make sure we have room to add a VLF offset.
   */
  private void ensureVlfCapacity() {
    int vlfOffsetsCapacity = 0;
    if (vlfOffsets != null) {
      vlfOffsetsCapacity = vlfOffsets.length;
    }
    if (vlfCount == vlfOffsetsCapacity) {
      int[] tmp = new int[vlfOffsetsCapacity + EXPAND_SIZE];
      for (int i = 0; i < vlfOffsetsCapacity; i++) {
        tmp[i] = vlfOffsets[i];
      }
      vlfOffsets = tmp;
    }
  }

  // only needed when creating a new type
  int getVlfCount() {
    return vlfCount;
  }

  @Override
  public <CT, VT extends CT> PdxWriter writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType) {
    return writeField(fieldName, fieldValue, fieldType, false);
  }

  @Override
  public <CT, VT extends CT> PdxWriter writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType, boolean onlyPortableObjects) {
    if (fieldType.equals(boolean.class)) {
      boolean v = false;
      if (fieldValue != null) {
        v = (Boolean) fieldValue;
      }
      writeBoolean(fieldName, v);
    } else if (fieldType.equals(byte.class)) {
      byte v = 0;
      if (fieldValue != null) {
        v = (Byte) fieldValue;
      }
      writeByte(fieldName, v);
    } else if (fieldType.equals(char.class)) {
      char v = 0;
      if (fieldValue != null) {
        v = (Character) fieldValue;
      }
      writeChar(fieldName, v);
    } else if (fieldType.equals(short.class)) {
      short v = 0;
      if (fieldValue != null) {
        v = (Short) fieldValue;
      }
      writeShort(fieldName, v);
    } else if (fieldType.equals(int.class)) {
      int v = 0;
      if (fieldValue != null) {
        v = (Integer) fieldValue;
      }
      writeInt(fieldName, v);
    } else if (fieldType.equals(long.class)) {
      long v = 0;
      if (fieldValue != null) {
        v = (Long) fieldValue;
      }
      writeLong(fieldName, v);
    } else if (fieldType.equals(float.class)) {
      float v = 0.0f;
      if (fieldValue != null) {
        v = (Float) fieldValue;
      }
      writeFloat(fieldName, v);
    } else if (fieldType.equals(double.class)) {
      double v = 0.0;
      if (fieldValue != null) {
        v = (Double) fieldValue;
      }
      writeDouble(fieldName, v);
    } else if (fieldType.equals(String.class)) {
      writeString(fieldName, (String) fieldValue);
    } else if (fieldType.isArray()) {
      if (fieldType.equals(boolean[].class)) {
        writeBooleanArray(fieldName, (boolean[]) fieldValue);
      } else if (fieldType.equals(byte[].class)) {
        writeByteArray(fieldName, (byte[]) fieldValue);
      } else if (fieldType.equals(char[].class)) {
        writeCharArray(fieldName, (char[]) fieldValue);
      } else if (fieldType.equals(short[].class)) {
        writeShortArray(fieldName, (short[]) fieldValue);
      } else if (fieldType.equals(int[].class)) {
        writeIntArray(fieldName, (int[]) fieldValue);
      } else if (fieldType.equals(long[].class)) {
        writeLongArray(fieldName, (long[]) fieldValue);
      } else if (fieldType.equals(float[].class)) {
        writeFloatArray(fieldName, (float[]) fieldValue);
      } else if (fieldType.equals(double[].class)) {
        writeDoubleArray(fieldName, (double[]) fieldValue);
      } else if (fieldType.equals(String[].class)) {
        writeStringArray(fieldName, (String[]) fieldValue);
      } else if (fieldType.equals(byte[][].class)) {
        writeArrayOfByteArrays(fieldName, (byte[][]) fieldValue);
      } else {
        writeObjectArray(fieldName, (Object[]) fieldValue, onlyPortableObjects);
      }
    } else if (fieldType.equals(Date.class)) {
      writeDate(fieldName, (Date) fieldValue);
    } else {
      writeObject(fieldName, fieldValue, onlyPortableObjects);
    }
    return this;
  }

  private void writeUnreadData() {
    if (unreadData != null) {
      unreadData.sendTo(this);
    }
  }

  public void writeRawField(PdxField ft, ByteSource data) {
    if (ft.isVariableLengthType()) {
      markVariableField();
    }
    updateMetaData(ft);
    os.write(data);
  }

  public void writeRawField(PdxField ft, byte[] data) {
    if (ft.isVariableLengthType()) {
      markVariableField();
    }
    updateMetaData(ft);
    os.write(data, 0, data.length);
  }

  void writeField(PdxField f, Object value) {
    switch (f.getFieldType()) {
      case CHAR:
        writeChar(null, (Character) value);
        break;
      case BOOLEAN:
        writeBoolean(null, (Boolean) value);
        break;
      case BYTE:
        writeByte(null, (Byte) value);
        break;
      case SHORT:
        writeShort(null, (Short) value);
        break;
      case INT:
        writeInt(null, (Integer) value);
        break;
      case FLOAT:
        writeFloat(null, (Float) value);
        break;
      case DOUBLE:
        writeDouble(null, (Double) value);
        break;
      case LONG:
        writeLong(null, (Long) value);
        break;
      case DATE:
        writeDate(null, (Date) value);
        break;
      case STRING:
        writeString(null, (String) value);
        break;
      case BOOLEAN_ARRAY:
        writeBooleanArray(null, (boolean[]) value);
        break;
      case CHAR_ARRAY:
        writeCharArray(null, (char[]) value);
        break;
      case BYTE_ARRAY:
        writeByteArray(null, (byte[]) value);
        break;
      case SHORT_ARRAY:
        writeShortArray(null, (short[]) value);
        break;
      case INT_ARRAY:
        writeIntArray(null, (int[]) value);
        break;
      case LONG_ARRAY:
        writeLongArray(null, (long[]) value);
        break;
      case FLOAT_ARRAY:
        writeFloatArray(null, (float[]) value);
        break;
      case DOUBLE_ARRAY:
        writeDoubleArray(null, (double[]) value);
        break;
      case STRING_ARRAY:
        writeStringArray(null, (String[]) value);
        break;
      case ARRAY_OF_BYTE_ARRAYS:
        writeArrayOfByteArrays(null, (byte[][]) value);
        break;
      case OBJECT_ARRAY:
        writeObjectArray(null, (Object[]) value);
        break;
      case OBJECT:
        writeObject(null, value);
        break;
      default:
        throw new InternalGemFireException("Unhandled field type " + f.getFieldType());
    }
  }

  private HeapDataOutputStream.LongUpdater lu;

  private void writeHeader() {
    os.write(DSCODE.PDX.toByte());
    lu = os.reserveLong(); // dummy length and type id
  }

  public boolean definingNewPdxType() {
    return newType != null;
  }

  // used by unit tests
  public void setDoExtraValidation(boolean v) {
    doExtraValidation = v;
  }

  private boolean doExtraValidation() {
    return doExtraValidation;
  }

  @Override
  public PdxWriter markIdentityField(String fieldName) {
    if (definingNewPdxType()) {
      PdxField ft = newType.getPdxField(fieldName);
      if (ft == null) {
        throw new PdxFieldDoesNotExistException(
            "Field " + fieldName + " must be written before calling markIdentityField");
      }
      ft.setIdentityField(true);
    } else if (doExtraValidation()) {
      PdxField ft = existingType.getPdxField(fieldName);
      if (ft == null) {
        throw new PdxFieldDoesNotExistException(
            "Field " + fieldName + " must be written before calling markIdentityField");
      } else if (!ft.isIdentityField()) {
        throw new PdxSerializationException("Expected field " + fieldName
            + " to not be marked as an identity field since it was not for the first serialization");
      }
    }
    return this;
  }

  @Override
  public PdxWriter writeUnreadFields(PdxUnreadFields unread) {
    if (fieldsWritten()) {
      throw new PdxFieldAlreadyExistsException(
          "writeUnreadFields must be called before any other fields are written.");
    }
    unreadData = (PdxUnreadData) unread;
    return this;
  }

  private void updateMetaData(String fieldName, FieldType type) {
    updateMetaData(fieldName, type, false);
  }

  private void updateMetaData(String fieldName, FieldType type, boolean isIdentityField) {
    beforeFieldWrite();
    if (definingNewPdxType()) {
      PdxField ft = new PdxField(fieldName, fieldId, vlfCount, type, isIdentityField);
      newType.addField(ft);
    } else if (doExtraValidation()) {
      PdxField ft = existingType.getPdxField(fieldName);
      if (ft == null) {
        throw new PdxSerializationException("Did not expect field " + fieldName
            + " to be serialized since it was not the first time this class was serialized.");
      }
      if (fieldId != ft.getFieldIndex()) {
        throw new PdxSerializationException(
            "Detected that the order in which the fields are serialized changed since the first time this class was serialized.");
      }
      if (!ft.getFieldType().equals(type)) {
        throw new PdxSerializationException("Expected field " + fieldName + " to be of type "
            + ft.getFieldType() + " not of type " + type);
      }
    }
  }

  private void updateMetaData(PdxField ft) {
    updateMetaData(ft.getFieldName(), ft.getFieldType(), ft.isIdentityField());
  }

  PdxInstance makePdxInstance() {
    final int DSCODE_SIZE = 1;
    final int LENGTH_SIZE = 4;
    final int PDX_TYPE_SIZE = 4;
    final int BYTES_TO_SKIP = DSCODE_SIZE + LENGTH_SIZE + PDX_TYPE_SIZE;
    ByteBuffer bb = os.toByteBuffer(BYTES_TO_SKIP);
    PdxType pt = newType;
    if (pt == null) {
      pt = existingType;
    }
    return new PdxInstanceImpl(pt, new PdxInputStream(bb), bb.limit());
  }

  public static boolean isPdx(byte[] valueBytes) {
    if (valueBytes == null || valueBytes.length < 1) {
      return false;
    }
    return valueBytes[0] == DSCODE.PDX.toByte();
  }

  public int position() {
    return os.size();
  }

}
