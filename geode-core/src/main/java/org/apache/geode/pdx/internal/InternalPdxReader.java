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

import java.util.Date;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;

/**
 * Adds additional methods for reading pdx fields for internal use.
 *
 * @since GemFire 6.6.2
 */
public interface InternalPdxReader extends PdxReader {
  PdxField getPdxField(String fieldName);

  char readChar(PdxField f);

  boolean readBoolean(PdxField f);

  byte readByte(PdxField f);

  short readShort(PdxField f);

  int readInt(PdxField f);

  long readLong(PdxField f);

  float readFloat(PdxField f);

  double readDouble(PdxField f);

  String readString(PdxField f);

  Object readObject(PdxField f);

  char[] readCharArray(PdxField f);

  boolean[] readBooleanArray(PdxField f);

  byte[] readByteArray(PdxField f);

  short[] readShortArray(PdxField f);

  int[] readIntArray(PdxField f);

  long[] readLongArray(PdxField f);

  float[] readFloatArray(PdxField f);

  double[] readDoubleArray(PdxField f);

  String[] readStringArray(PdxField f);

  Object[] readObjectArray(PdxField f);

  byte[][] readArrayOfByteArrays(PdxField f);

  Date readDate(PdxField f);

  char readChar();

  boolean readBoolean();

  byte readByte();

  short readShort();

  int readInt();

  long readLong();

  float readFloat();

  double readDouble();

  String readString();

  Object readObject();

  char[] readCharArray();

  boolean[] readBooleanArray();

  byte[] readByteArray();

  short[] readShortArray();

  int[] readIntArray();

  long[] readLongArray();

  float[] readFloatArray();

  double[] readDoubleArray();

  String[] readStringArray();

  Object[] readObjectArray();

  byte[][] readArrayOfByteArrays();

  Date readDate();

  PdxType getPdxType();

  void orderedDeserialize(Object obj, AutoClassInfo ci);

}
