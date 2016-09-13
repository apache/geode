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

import java.util.Date;

import org.apache.geode.pdx.PdxFieldTypeMismatchException;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;

/**
 * Adds additional methods for reading pdx fields for internal use.
 * @since GemFire 6.6.2
 */
public interface InternalPdxReader extends PdxReader {
  public PdxField getPdxField(String fieldName);
  
  public char readChar(PdxField f);
  
  public boolean readBoolean(PdxField f);
  public byte readByte(PdxField f);
  public short readShort(PdxField f);
  public int readInt(PdxField f);
  public long readLong(PdxField f);
  public float readFloat(PdxField f);
  public double readDouble(PdxField f);
  public String readString(PdxField f);
  public Object readObject(PdxField f);
  public char[] readCharArray(PdxField f);
  public boolean[] readBooleanArray(PdxField f);
  public byte[] readByteArray(PdxField f);
  public short[] readShortArray(PdxField f);
  public int[] readIntArray(PdxField f);
  public long[] readLongArray(PdxField f);
  public float[] readFloatArray(PdxField f);
  public double[] readDoubleArray(PdxField f) ;
  public String[] readStringArray(PdxField f);
  public Object[] readObjectArray(PdxField f);
  public byte[][] readArrayOfByteArrays(PdxField f);
  public Date readDate(PdxField f);
  
  public char readChar();
  public boolean readBoolean();
  public byte readByte();
  public short readShort();
  public int readInt();
  public long readLong();
  public float readFloat();
  public double readDouble();
  public String readString();
  public Object readObject();
  public char[] readCharArray();
  public boolean[] readBooleanArray();
  public byte[] readByteArray();
  public short[] readShortArray();
  public int[] readIntArray();
  public long[] readLongArray();
  public float[] readFloatArray();
  public double[] readDoubleArray() ;
  public String[] readStringArray();
  public Object[] readObjectArray();
  public byte[][] readArrayOfByteArrays();
  public Date readDate();

  public PdxType getPdxType();

  public void orderedDeserialize(Object obj, AutoClassInfo ci);
 
}
