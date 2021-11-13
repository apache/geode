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
    this.os = out;
    this.headerOffset = this.os.size();
    this.aci = null;
  }

  PdxWriterImpl(PdxType pdxType, PdxOutputStream out) {
    this.tr = null;
    this.pdx = null;
    this.os = out;
    this.existingType = pdxType;
    this.headerOffset = this.os.size();
    this.aci = null;
  }

  PdxWriterImpl(PdxType pt, TypeRegistry tr, PdxOutputStream out) {
    this.tr = tr;
    this.pdx = null;
    this.os = out;
    this.newType = pt;
    this.headerOffset = this.os.size();
    this.aci = null;
  }

  public PdxWriterImpl(TypeRegistry tr, Object pdx, AutoClassInfo aci, PdxOutputStream os) {
    this.tr = tr;
    this.pdx = pdx;
    this.os = os;
    this.headerOffset = this.os.size();
    this.aci = aci;
  }

  private boolean fieldsWritten() {
    return this.fieldId >= 0;
  }

  private void beforeFieldWrite() {
    ++this.fieldId;
    if (this.fieldId > 0) {
      // already wrote first field
      return;
    }
    initialize();
  }

  private void initialize() {
    writeHeader();
    if (this.existingType != null) {
      // PdxInstance is using us to flush its dirty fields
      return;
    }
    if (definingNewPdxType()) {
      // PdxInstanceFactoryImpl is using us
      return;
    }
    PdxUnreadData ud = initUnreadData();
    if (ud == null && this.pdx != null) {
      if (this.aci != null) {
        this.existingType = aci.getSerializedType();
      } else {
        this.existingType = this.tr.getExistingType(this.pdx);
      }
    } else if (ud != null) {
      this.existingType = ud.getSerializedType();
    }

    if (this.existingType != null) {
      int c = this.existingType.getVariableLengthFieldCount();
      if (c > 0) {
        this.vlfOffsets = new int[c];
      }
    } else if (this.pdx != null) {
      this.newType = new PdxType(this.pdx.getClass().getName(), true);
    }
  }

  private boolean unreadDataInitialized = false;

  PdxUnreadData initUnreadData() {
    if (this.unreadDataInitialized) {
      return this.unreadData;
    }
    this.unreadDataInitialized = true;
    if (this.tr == null) {
      // We are being PdxSerializer serialized.
      // Now is the time to initialize tr.
      this.tr = GemFireCacheImpl.getForPdx("Could not access Pdx registry").getPdxRegistry();
    }
    PdxUnreadData ud = this.unreadData;
    if (ud == null && this.pdx != null) {
      ud = this.tr.getUnreadData(this.pdx);
      this.unreadData = ud;
    }
    return ud;
  }

  @Override
  public PdxWriter writeChar(String fieldName, char value) {
    updateMetaData(fieldName, FieldType.CHAR);
    this.os.writeChar(value);
    return this;
  }

  public void writeChar(char value) {
    beforeFieldWrite();
    this.os.writeChar(value);
  }

  @Override
  public PdxWriter writeBoolean(String fieldName, boolean value) {
    updateMetaData(fieldName, FieldType.BOOLEAN);
    this.os.writeByte((value) ? 0x1 : 0x0);
    return this;
  }

  public void writeBoolean(boolean value) {
    beforeFieldWrite();
    this.os.writeByte((value) ? 0x1 : 0x0);
  }

  @Override
  public PdxWriter writeByte(String fieldName, byte value) {
    updateMetaData(fieldName, FieldType.BYTE);
    this.os.writeByte(value);
    return this;
  }

  public void writeByte(byte value) {
    beforeFieldWrite();
    this.os.writeByte(value);
  }

  @Override
  public PdxWriter writeShort(String fieldName, short value) {
    updateMetaData(fieldName, FieldType.SHORT);
    this.os.writeShort(value);
    return this;
  }

  public void writeShort(short value) {
    beforeFieldWrite();
    this.os.writeShort(value);
  }

  @Override
  public PdxWriter writeInt(String fieldName, int value) {
    updateMetaData(fieldName, FieldType.INT);
    this.os.writeInt(value);
    return this;
  }

  public void writeInt(int value) {
    beforeFieldWrite();
    this.os.writeInt(value);
  }

  @Override
  public PdxWriter writeLong(String fieldName, long value) {
    updateMetaData(fieldName, FieldType.LONG);
    this.os.writeLong(value);
    return this;
  }

  public void writeLong(long value) {
    beforeFieldWrite();
    this.os.writeLong(value);
  }

  @Override
  public PdxWriter writeFloat(String fieldName, float value) {
    updateMetaData(fieldName, FieldType.FLOAT);
    this.os.writeFloat(value);
    return this;
  }

  public void writeFloat(float value) {
    beforeFieldWrite();
    this.os.writeFloat(value);
  }

  @Override
  public PdxWriter writeDouble(String fieldName, double value) {
    updateMetaData(fieldName, FieldType.DOUBLE);
    this.os.writeDouble(value);
    return this;
  }

  public void writeDouble(double value) {
    beforeFieldWrite();
    this.os.writeDouble(value);
  }

  @Override
  public PdxWriter writeDate(String fieldName, Date date) {
    if (date != null && !Date.class.equals(date.getClass())) {
      // fix for bug 43717
      throw new IllegalArgumentException(
          "writeDate only accepts instances of Date. Subclasses are not supported. Use writeObject for subclasses of Date.");
    }
    updateMetaData(fieldName, FieldType.DATE);
    this.os.writeDate(date);
    return this;
  }

  public void writeDate(Date date) {
    if (date != null && !Date.class.equals(date.getClass())) {
      // fix for bug 43717
      throw new IllegalArgumentException(
          "writeDate only accepts instances of Date. Subclasses are not supported. Use writeObject for subclasses of Date.");
    }
    beforeFieldWrite();
    this.os.writeDate(date);
  }

  @Override
  public PdxWriter writeString(String fieldName, String value) {
    markVariableField();
    updateMetaData(fieldName, FieldType.STRING);
    this.os.writeString(value);
    return this;
  }

  public void writeString(String value) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeString(value);
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
    this.os.writeObject(object, onlyPortableObjects);
    return this;
  }

  public void writeObject(Object object, boolean onlyPortableObjects) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeObject(object, onlyPortableObjects);
  }

  @Override
  public PdxWriter writeBooleanArray(String fieldName, boolean[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.BOOLEAN_ARRAY);
    this.os.writeBooleanArray(array);
    return this;
  }

  public void writeBooleanArray(boolean[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeBooleanArray(array);
  }

  @Override
  public PdxWriter writeCharArray(String fieldName, char[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.CHAR_ARRAY);
    this.os.writeCharArray(array);
    return this;
  }

  public void writeCharArray(char[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeCharArray(array);
  }

  @Override
  public PdxWriter writeByteArray(String fieldName, byte[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.BYTE_ARRAY);
    this.os.writeByteArray(array);
    return this;
  }

  public void writeByteArray(byte[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeByteArray(array);
  }

  @Override
  public PdxWriter writeShortArray(String fieldName, short[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.SHORT_ARRAY);
    this.os.writeShortArray(array);
    return this;
  }

  public void writeShortArray(short[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeShortArray(array);
  }

  @Override
  public PdxWriter writeIntArray(String fieldName, int[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.INT_ARRAY);
    this.os.writeIntArray(array);
    return this;
  }

  public void writeIntArray(int[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeIntArray(array);
  }

  @Override
  public PdxWriter writeLongArray(String fieldName, long[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.LONG_ARRAY);
    this.os.writeLongArray(array);
    return this;
  }

  public void writeLongArray(long[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeLongArray(array);
  }

  @Override
  public PdxWriter writeFloatArray(String fieldName, float[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.FLOAT_ARRAY);
    this.os.writeFloatArray(array);
    return this;
  }

  public void writeFloatArray(float[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeFloatArray(array);
  }

  @Override
  public PdxWriter writeDoubleArray(String fieldName, double[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.DOUBLE_ARRAY);
    this.os.writeDoubleArray(array);
    return this;
  }

  public void writeDoubleArray(double[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeDoubleArray(array);
  }

  @Override
  public PdxWriter writeStringArray(String fieldName, String[] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.STRING_ARRAY);
    this.os.writeStringArray(array);
    return this;
  }

  public void writeStringArray(String[] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeStringArray(array);
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
    this.os.writeObjectArray(array, onlyPortableObjects);
    return this;
  }

  public void writeObjectArray(Object[] array, boolean onlyPortableObjects) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeObjectArray(array, onlyPortableObjects);
  }

  @Override
  public PdxWriter writeArrayOfByteArrays(String fieldName, byte[][] array) {
    markVariableField();
    updateMetaData(fieldName, FieldType.ARRAY_OF_BYTE_ARRAYS);
    this.os.writeArrayOfByteArrays(array);
    return this;
  }

  public void writeArrayOfByteArrays(byte[][] array) {
    markVariableField();
    beforeFieldWrite();
    this.os.writeArrayOfByteArrays(array);
  }

  private boolean alreadyGenerated = false;

  /**
   * Must be invoked only after {@link PdxSerializable#toData(PdxWriter)}
   *
   * @return total number of bytes serialized for this pdx
   */
  public int completeByteStreamGeneration() {
    if (!this.alreadyGenerated) {
      this.alreadyGenerated = true;
      if (!fieldsWritten()) {
        initialize();
      }
      writeUnreadData();
      appendOffsets();
      int typeId;
      if (definingNewPdxType()) {
        this.newType.initialize(this);
        if (this.unreadData != null && !this.unreadData.isEmpty()) {
          // We created a new type that had unreadData.
          // In this case we don't define a local type
          // but we do set the serialized type.
          this.newType = this.tr.defineType(newType);
          typeId = this.newType.getTypeId();
          this.unreadData.setSerializedType(newType);
        } else {
          this.newType = this.tr.defineLocalType(this.pdx, newType);
          typeId = this.newType.getTypeId();
        }
      } else {
        if (doExtraValidation()) {
          int fieldCount = this.fieldId + 1;
          if (this.existingType.getFieldCount() != fieldCount) {
            throw new PdxSerializationException("Expected the number of fields for class "
                + this.existingType.getClassName() + " to be " + this.existingType.getFieldCount()
                + " but instead it was " + fieldCount);
          }
        }
        typeId = this.existingType.getTypeId();
      }

      // Now write length of the byte stream (does not include bytes for DSCODE and the length
      // itself.)
      long bits = ((long) getCurrentOffset()) << 32 | (0x00000000FFFFFFFFL & typeId); // fixes 45005
      this.lu.update(bits);
    } // !alreadyGenerated

    return getCurrentOffset() + 1; // +1 for DSCODE.PDX.toByte()
  }

  /**
   * Returns the pdx type that can be used by the auto serializer to always serialize this class.
   */
  public PdxType getAutoPdxType() {
    if (this.unreadData != null && !this.unreadData.isEmpty()) {
      return null;
    }
    completeByteStreamGeneration();
    if (definingNewPdxType()) {
      return this.newType;
    } else {
      return this.existingType;
    }
  }

  public PdxType getPdxType() {
    return newType;
  }

  /**
   * @return the offset to the byte of the first field
   */
  private int getBaseOffset() {
    return this.headerOffset + DataSize.BYTE_SIZE + (DataSize.INTEGER_SIZE * 2);
  }

  private int getCurrentOffset() {
    return this.os.size() - getBaseOffset();
  }

  /**
   * Must be invoked only after {@link PdxSerializable#toData(PdxWriter)}
   */
  private void appendOffsets() {
    int fieldDataSize = getCurrentOffset();
    // Take the list of offsets and append it in reverse order.
    byte sizeOfOffset = getSizeOfOffset(this.vlfCount, fieldDataSize);
    for (int i = (this.vlfCount - 1); i >= 0; i--) {
      switch (sizeOfOffset) {
        case 1:
          this.os.write((byte) this.vlfOffsets[i]);
          break;
        case 2:
          this.os.writeShort((short) this.vlfOffsets[i]);
          break;
        case 4:
          this.os.writeInt(this.vlfOffsets[i]);
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
    this.os.sendTo(out);
  }

  public byte[] toByteArray() {
    return this.os.toByteArray();
  }

  private void markVariableField() {
    if (!this.hasSeenFirstVlf) {
      this.hasSeenFirstVlf = true;
    } else {
      ensureVlfCapacity();
      this.vlfOffsets[this.vlfCount] = getCurrentOffset();
      this.vlfCount++;
    }
  }

  /**
   * Make sure we have room to add a VLF offset.
   */
  private void ensureVlfCapacity() {
    int vlfOffsetsCapacity = 0;
    if (this.vlfOffsets != null) {
      vlfOffsetsCapacity = this.vlfOffsets.length;
    }
    if (this.vlfCount == vlfOffsetsCapacity) {
      int[] tmp = new int[vlfOffsetsCapacity + EXPAND_SIZE];
      for (int i = 0; i < vlfOffsetsCapacity; i++) {
        tmp[i] = this.vlfOffsets[i];
      }
      this.vlfOffsets = tmp;
    }
  }

  // only needed when creating a new type
  int getVlfCount() {
    return this.vlfCount;
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
    if (this.unreadData != null) {
      this.unreadData.sendTo(this);
    }
  }

  public void writeRawField(PdxField ft, ByteSource data) {
    if (ft.isVariableLengthType()) {
      markVariableField();
    }
    updateMetaData(ft);
    this.os.write(data);
  }

  public void writeRawField(PdxField ft, byte[] data) {
    if (ft.isVariableLengthType()) {
      markVariableField();
    }
    updateMetaData(ft);
    this.os.write(data, 0, data.length);
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

  public static volatile boolean breakIt = false;
  private void writeHeader() {
    if (breakIt) {
      this.os.write((byte) 0);
    } else {
      this.os.write(DSCODE.PDX.toByte());
    }
    this.lu = this.os.reserveLong(); // dummy length and type id
  }

  public boolean definingNewPdxType() {
    return this.newType != null;
  }

  // used by unit tests
  public void setDoExtraValidation(boolean v) {
    this.doExtraValidation = v;
  }

  private boolean doExtraValidation() {
    return this.doExtraValidation;
  }

  @Override
  public PdxWriter markIdentityField(String fieldName) {
    if (definingNewPdxType()) {
      PdxField ft = this.newType.getPdxField(fieldName);
      if (ft == null) {
        throw new PdxFieldDoesNotExistException(
            "Field " + fieldName + " must be written before calling markIdentityField");
      }
      ft.setIdentityField(true);
    } else if (doExtraValidation()) {
      PdxField ft = this.existingType.getPdxField(fieldName);
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
    this.unreadData = (PdxUnreadData) unread;
    return this;
  }

  private void updateMetaData(String fieldName, FieldType type) {
    updateMetaData(fieldName, type, false);
  }

  private void updateMetaData(String fieldName, FieldType type, boolean isIdentityField) {
    beforeFieldWrite();
    if (definingNewPdxType()) {
      PdxField ft = new PdxField(fieldName, this.fieldId, this.vlfCount, type, isIdentityField);
      this.newType.addField(ft);
    } else if (doExtraValidation()) {
      PdxField ft = this.existingType.getPdxField(fieldName);
      if (ft == null) {
        throw new PdxSerializationException("Did not expect field " + fieldName
            + " to be serialized since it was not the first time this class was serialized.");
      }
      if (this.fieldId != ft.getFieldIndex()) {
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
    ByteBuffer bb = this.os.toByteBuffer(BYTES_TO_SKIP);
    PdxType pt = this.newType;
    if (pt == null) {
      pt = this.existingType;
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
    return this.os.size();
  }

}
