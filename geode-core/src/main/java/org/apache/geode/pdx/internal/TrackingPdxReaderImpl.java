/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.pdx.internal;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxUnreadFields;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;
import org.apache.geode.pdx.internal.AutoSerializableManager.PdxFieldWrapper;

/**
 * Used to track what fields are actually read by the user's code.
 * We want to know what fields are not read so that we can preserve them.
 * 
 * @since GemFire 6.6
 */
public class TrackingPdxReaderImpl implements InternalPdxReader {

  /**
   * The PdxReaderImpl that we wrap. Every method needs to be
   * forwarded to this method.
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
  public char readChar(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readChar(fieldName);
  }
  public boolean readBoolean(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readBoolean(fieldName);
  }
  public byte readByte(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readByte(fieldName);
  }
  public short readShort(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readShort(fieldName);
  }
  public int readInt(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readInt(fieldName);
  }
  public long readLong(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readLong(fieldName);
  }
  public float readFloat(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readFloat(fieldName);
  }
  public double readDouble(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readDouble(fieldName);
  }
  public String readString(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readString(fieldName);
  }
  public Object readObject(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readObject(fieldName);
  }
  public char[] readCharArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readCharArray(fieldName);
  }
  public boolean[] readBooleanArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readBooleanArray(fieldName);
  }
  public byte[] readByteArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readByteArray(fieldName);
  }
  public short[] readShortArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readShortArray(fieldName);
  }
  public int[] readIntArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readIntArray(fieldName);
  }
  public long[] readLongArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readLongArray(fieldName);
  }
  public float[] readFloatArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readFloatArray(fieldName);
  }
  public double[] readDoubleArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readDoubleArray(fieldName);
  }
  public String[] readStringArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readStringArray(fieldName);
  }
  public Object[] readObjectArray(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readObjectArray(fieldName);
  }
  public byte[][] readArrayOfByteArrays(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readArrayOfByteArrays(fieldName);
  }
  public Date readDate(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readDate(fieldName);
  }
  public boolean hasField(String fieldName) {
    return this.pdxReader.hasField(fieldName);
  }
  public Object readField(String fieldName) {
    this.readFields.add(fieldName);
    return this.pdxReader.readField(fieldName);
  }
  
  public boolean isIdentityField(String fieldName) {
    return this.pdxReader.isIdentityField(fieldName);
  }
  /**
   * Returns the indexes of the fields not read during deserialization.
   * @return the indexes of the unread fields
   */
  private int[] generateUnreadDataFieldIndexes() {
    PdxType blobType = this.pdxReader.getPdxType();
    List<Integer> unreadFields = blobType.getUnreadFieldIndexes(this.readFields);
    int[] unreadFieldIndexes = new int[unreadFields.size()];
    if (!unreadFields.isEmpty()) {
      int i = 0;
      for (int fieldIndex: unreadFields) {
        unreadFieldIndexes[i] = fieldIndex;
        i++;
      }
    }
    return unreadFieldIndexes;
  }
  public PdxUnreadFields readUnreadFields() {
    return this.pdxReader.readUnreadFields(); 
  }
  
  PdxUnreadData internalReadUnreadFields(PdxUnreadData ud) {
    int[] unreadIndexes = generateUnreadDataFieldIndexes();
    if (unreadIndexes.length > 0) {
      UnreadPdxType unreadLocalPdxType = new UnreadPdxType(this.pdxReader.getPdxType(), unreadIndexes);
      this.tr.defineUnreadType(this.pdxClass, unreadLocalPdxType);
      ud.initialize(unreadLocalPdxType, this.pdxReader);
      return ud;
    } else {
      // Remember that this type does not have any unread data.
      UnreadPdxType unreadLocalPdxType = new UnreadPdxType(this.pdxReader.getPdxType(), null);
      this.tr.defineUnreadType(this.pdxClass, unreadLocalPdxType);
      return null;
    }
  }
  
  public PdxField getPdxField(String fieldName) {
    return this.pdxReader.getPdxField(fieldName);
  }
  
  public char readChar(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readChar(f);
  }
  public boolean readBoolean(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readBoolean(f);
  }
  public byte readByte(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readByte(f);
  }
  public short readShort(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readShort(f);
  }
  public int readInt(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readInt(f);
  }
  public long readLong(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readLong(f);
  }
  public float readFloat(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readFloat(f);
  }
  public double readDouble(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readDouble(f);
  }
  public String readString(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readString(f);
  }
  public Object readObject(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readObject(f);
  }
  public char[] readCharArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readCharArray(f);
  }
  public boolean[] readBooleanArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readBooleanArray(f);
  }
  public byte[] readByteArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readByteArray(f);
  }
  public short[] readShortArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readShortArray(f);
  }
  public int[] readIntArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readIntArray(f);
  }
  public long[] readLongArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readLongArray(f);
  }
  public float[] readFloatArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readFloatArray(f);
  }
  public double[] readDoubleArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readDoubleArray(f);
  }
  public String[] readStringArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readStringArray(f);
  }
  public Object[] readObjectArray(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readObjectArray(f);
  }
  public byte[][] readArrayOfByteArrays(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readArrayOfByteArrays(f);
  }
  public Date readDate(PdxField f) {
    this.readFields.add(f.getFieldName());
    return this.pdxReader.readDate(f);
  }

  public char readChar() {
    return this.pdxReader.readChar();
  }
  public boolean readBoolean() {
    return this.pdxReader.readBoolean();
  }
  public byte readByte() {
    return this.pdxReader.readByte();
  }
  public short readShort() {
    return this.pdxReader.readShort();
  }
  public int readInt() {
    return this.pdxReader.readInt();
  }
  public long readLong() {
    return this.pdxReader.readLong();
  }
  public float readFloat() {
    return this.pdxReader.readFloat();
  }
  public double readDouble() {
    return this.pdxReader.readDouble();
  }
  public String readString() {
    return this.pdxReader.readString();
  }
  public Object readObject() {
    return this.pdxReader.readObject();
  }
  public char[] readCharArray() {
    return this.pdxReader.readCharArray();
  }
  public boolean[] readBooleanArray() {
    return this.pdxReader.readBooleanArray();
  }
  public byte[] readByteArray() {
    return this.pdxReader.readByteArray();
  }
  public short[] readShortArray() {
    return this.pdxReader.readShortArray();
  }
  public int[] readIntArray() {
    return this.pdxReader.readIntArray();
  }
  public long[] readLongArray() {
    return this.pdxReader.readLongArray();
  }
  public float[] readFloatArray() {
    return this.pdxReader.readFloatArray();
  }
  public double[] readDoubleArray() {
    return this.pdxReader.readDoubleArray();
  }
  public String[] readStringArray() {
    return this.pdxReader.readStringArray();
  }
  public Object[] readObjectArray() {
    return this.pdxReader.readObjectArray();
  }
  public byte[][] readArrayOfByteArrays() {
    return this.pdxReader.readArrayOfByteArrays();
  }
  public Date readDate() {
    return this.pdxReader.readDate();
  }
  public PdxType getPdxType() {
    return this.pdxReader.getPdxType();
  }
  public void orderedDeserialize(Object obj, AutoClassInfo ci) {
    this.pdxReader.orderedDeserialize(obj, ci);
    for (PdxFieldWrapper f: ci.getFields()) {
      this.readFields.add(f.getName());
    }
  }
}
