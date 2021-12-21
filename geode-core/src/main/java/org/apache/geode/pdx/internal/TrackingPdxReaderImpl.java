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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.geode.pdx.PdxUnreadFields;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;
import org.apache.geode.pdx.internal.AutoSerializableManager.PdxFieldWrapper;

/**
 * Used to track what fields are actually read by the user's code. We want to know what fields are
 * not read so that we can preserve them.
 *
 * @since GemFire 6.6
 */
public class TrackingPdxReaderImpl implements InternalPdxReader {

  /**
   * The PdxReaderImpl that we wrap. Every method needs to be forwarded to this method.
   */
  private final PdxReaderImpl pdxReader;
  private final TypeRegistry tr;
  private final Class<?> pdxClass;
  private final ArrayList<String> readFields = new ArrayList<String>();

  public TrackingPdxReaderImpl(PdxReaderImpl pdxReader, TypeRegistry tr, Class<?> pdxClass) {
    this.pdxReader = pdxReader;
    this.tr = tr;
    this.pdxClass = pdxClass;
  }

  @Override
  public char readChar(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readChar(fieldName);
  }

  @Override
  public boolean readBoolean(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readBoolean(fieldName);
  }

  @Override
  public byte readByte(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readByte(fieldName);
  }

  @Override
  public short readShort(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readShort(fieldName);
  }

  @Override
  public int readInt(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readInt(fieldName);
  }

  @Override
  public long readLong(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readLong(fieldName);
  }

  @Override
  public float readFloat(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readFloat(fieldName);
  }

  @Override
  public double readDouble(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readDouble(fieldName);
  }

  @Override
  public String readString(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readString(fieldName);
  }

  @Override
  public Object readObject(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readObject(fieldName);
  }

  @Override
  public char[] readCharArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readCharArray(fieldName);
  }

  @Override
  public boolean[] readBooleanArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readBooleanArray(fieldName);
  }

  @Override
  public byte[] readByteArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readByteArray(fieldName);
  }

  @Override
  public short[] readShortArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readShortArray(fieldName);
  }

  @Override
  public int[] readIntArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readIntArray(fieldName);
  }

  @Override
  public long[] readLongArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readLongArray(fieldName);
  }

  @Override
  public float[] readFloatArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readFloatArray(fieldName);
  }

  @Override
  public double[] readDoubleArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readDoubleArray(fieldName);
  }

  @Override
  public String[] readStringArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readStringArray(fieldName);
  }

  @Override
  public Object[] readObjectArray(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readObjectArray(fieldName);
  }

  @Override
  public byte[][] readArrayOfByteArrays(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readArrayOfByteArrays(fieldName);
  }

  @Override
  public Date readDate(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readDate(fieldName);
  }

  @Override
  public boolean hasField(String fieldName) {
    return pdxReader.hasField(fieldName);
  }

  @Override
  public Object readField(String fieldName) {
    readFields.add(fieldName);
    return pdxReader.readField(fieldName);
  }

  @Override
  public boolean isIdentityField(String fieldName) {
    return pdxReader.isIdentityField(fieldName);
  }

  /**
   * Returns the indexes of the fields not read during deserialization.
   *
   * @return the indexes of the unread fields
   */
  private int[] generateUnreadDataFieldIndexes() {
    PdxType blobType = pdxReader.getPdxType();
    List<Integer> unreadFields = blobType.getUnreadFieldIndexes(readFields);
    int[] unreadFieldIndexes = new int[unreadFields.size()];
    if (!unreadFields.isEmpty()) {
      int i = 0;
      for (int fieldIndex : unreadFields) {
        unreadFieldIndexes[i] = fieldIndex;
        i++;
      }
    }
    return unreadFieldIndexes;
  }

  @Override
  public PdxUnreadFields readUnreadFields() {
    return pdxReader.readUnreadFields();
  }

  PdxUnreadData internalReadUnreadFields(PdxUnreadData ud) {
    int[] unreadIndexes = generateUnreadDataFieldIndexes();
    if (unreadIndexes.length > 0) {
      UnreadPdxType unreadLocalPdxType =
          new UnreadPdxType(pdxReader.getPdxType(), unreadIndexes);
      tr.defineUnreadType(pdxClass, unreadLocalPdxType);
      ud.initialize(unreadLocalPdxType, pdxReader);
      return ud;
    } else {
      // Remember that this type does not have any unread data.
      UnreadPdxType unreadLocalPdxType = new UnreadPdxType(pdxReader.getPdxType(), null);
      tr.defineUnreadType(pdxClass, unreadLocalPdxType);
      return null;
    }
  }

  @Override
  public PdxField getPdxField(String fieldName) {
    return pdxReader.getPdxField(fieldName);
  }

  @Override
  public char readChar(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readChar(f);
  }

  @Override
  public boolean readBoolean(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readBoolean(f);
  }

  @Override
  public byte readByte(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readByte(f);
  }

  @Override
  public short readShort(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readShort(f);
  }

  @Override
  public int readInt(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readInt(f);
  }

  @Override
  public long readLong(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readLong(f);
  }

  @Override
  public float readFloat(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readFloat(f);
  }

  @Override
  public double readDouble(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readDouble(f);
  }

  @Override
  public String readString(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readString(f);
  }

  @Override
  public Object readObject(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readObject(f);
  }

  @Override
  public char[] readCharArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readCharArray(f);
  }

  @Override
  public boolean[] readBooleanArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readBooleanArray(f);
  }

  @Override
  public byte[] readByteArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readByteArray(f);
  }

  @Override
  public short[] readShortArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readShortArray(f);
  }

  @Override
  public int[] readIntArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readIntArray(f);
  }

  @Override
  public long[] readLongArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readLongArray(f);
  }

  @Override
  public float[] readFloatArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readFloatArray(f);
  }

  @Override
  public double[] readDoubleArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readDoubleArray(f);
  }

  @Override
  public String[] readStringArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readStringArray(f);
  }

  @Override
  public Object[] readObjectArray(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readObjectArray(f);
  }

  @Override
  public byte[][] readArrayOfByteArrays(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readArrayOfByteArrays(f);
  }

  @Override
  public Date readDate(PdxField f) {
    readFields.add(f.getFieldName());
    return pdxReader.readDate(f);
  }

  @Override
  public char readChar() {
    return pdxReader.readChar();
  }

  @Override
  public boolean readBoolean() {
    return pdxReader.readBoolean();
  }

  @Override
  public byte readByte() {
    return pdxReader.readByte();
  }

  @Override
  public short readShort() {
    return pdxReader.readShort();
  }

  @Override
  public int readInt() {
    return pdxReader.readInt();
  }

  @Override
  public long readLong() {
    return pdxReader.readLong();
  }

  @Override
  public float readFloat() {
    return pdxReader.readFloat();
  }

  @Override
  public double readDouble() {
    return pdxReader.readDouble();
  }

  @Override
  public String readString() {
    return pdxReader.readString();
  }

  @Override
  public Object readObject() {
    return pdxReader.readObject();
  }

  @Override
  public char[] readCharArray() {
    return pdxReader.readCharArray();
  }

  @Override
  public boolean[] readBooleanArray() {
    return pdxReader.readBooleanArray();
  }

  @Override
  public byte[] readByteArray() {
    return pdxReader.readByteArray();
  }

  @Override
  public short[] readShortArray() {
    return pdxReader.readShortArray();
  }

  @Override
  public int[] readIntArray() {
    return pdxReader.readIntArray();
  }

  @Override
  public long[] readLongArray() {
    return pdxReader.readLongArray();
  }

  @Override
  public float[] readFloatArray() {
    return pdxReader.readFloatArray();
  }

  @Override
  public double[] readDoubleArray() {
    return pdxReader.readDoubleArray();
  }

  @Override
  public String[] readStringArray() {
    return pdxReader.readStringArray();
  }

  @Override
  public Object[] readObjectArray() {
    return pdxReader.readObjectArray();
  }

  @Override
  public byte[][] readArrayOfByteArrays() {
    return pdxReader.readArrayOfByteArrays();
  }

  @Override
  public Date readDate() {
    return pdxReader.readDate();
  }

  @Override
  public PdxType getPdxType() {
    return pdxReader.getPdxType();
  }

  @Override
  public void orderedDeserialize(Object obj, AutoClassInfo ci) {
    pdxReader.orderedDeserialize(obj, ci);
    for (PdxFieldWrapper f : ci.getFields()) {
      readFields.add(f.getName());
    }
  }
}
