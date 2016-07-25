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
/**
 * 
 */
package com.gemstone.gemfire.pdx.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.pdx.PdxSerializationException;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream;
import com.gemstone.gemfire.internal.tcp.ImmutableByteBufferInputStream;

/**
 * Used by PdxReaderImpl to manage the raw bytes of a PDX.
 * 
 * @since GemFire 6.6
 */
public class PdxInputStream extends ImmutableByteBufferInputStream {

  /**
   * Create a pdx input stream by whose contents are the first length
   * bytes from the given input stream.
   * @param existing the input stream whose content will go into this stream. Note that this existing stream will be read by this class (a copy is not made) so it should not be changed externally.
   * @param length the number of bytes to put in this stream
   */
  public PdxInputStream(ByteBufferInputStream existing, int length) {
    super(existing, length);
  }

  /**
   * Create a pdx input stream whose contents are the given bytes
   * @param bytes the content of this stream. Note that this byte array will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public PdxInputStream(byte[] bytes) {
    super(bytes);
  }

  /**
   * Create a pdx input stream whose contents are the given bytes
   * @param bb the content of this stream. Note that bb will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public PdxInputStream(ByteBuffer bb) {
    super(bb);
  }

  /**
   * Create a pdx input stream by copying another. A somewhat shallow copy is made.
   * @param copy the input stream to copy. Note that this copy stream will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public PdxInputStream(PdxInputStream copy) {
    super(copy);
  }
  
  public PdxInputStream() {
    // for serialization
  }

  public PdxInputStream(StoredObject blob) {
    super(blob);
  }

  public String readString(int positionForField) {
      position(positionForField);
      return readString();
  }

  public Object readObject(int positionForField) {
      position(positionForField);
      return readObject();
  }

  public char[] readCharArray(int positionForField) {
      position(positionForField);
      return readCharArray();
  }

  public boolean[] readBooleanArray(int positionForField) {
      position(positionForField);
      return readBooleanArray();
  }

  public byte[] readByteArray(int positionForField) {
      position(positionForField);
      return readByteArray();
  }

  public short[] readShortArray(int positionForField) {
      position(positionForField);
      return readShortArray();
  }

  public int[] readIntArray(int positionForField) {
      position(positionForField);
      return readIntArray();
  }

  public long[] readLongArray(int positionForField) {
      position(positionForField);
      return readLongArray();
  }

  public float[] readFloatArray(int positionForField) {
      position(positionForField);
      return readFloatArray();
  }

  public double[] readDoubleArray(int positionForField) {
      position(positionForField);
      return readDoubleArray();
  }

  public String[] readStringArray(int positionForField) {
      position(positionForField);
      return readStringArray();
  }

  public Object[] readObjectArray(int positionForField) {
      position(positionForField);
      return readObjectArray();
  }

  public byte[][] readArrayOfByteArrays(int positionForField) {
      position(positionForField);
      return readArrayOfByteArrays();
  }

  public Date readDate(int offset) {
    long time = readLong(offset);
    return convertLongToDate(time);
  }

  @Override
  public boolean readBoolean(int pos) {
    try {
      return super.readBoolean(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX boolean field", e);
    }
  }

  @Override
  public byte readByte(int pos) {
    try {
      return super.readByte(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX byte field", e);
    }
  }

  @Override
  public char readChar(int pos) {
    try {
      return super.readChar(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX char field", e);
    }

  }

  @Override
  public double readDouble(int pos) {
    try {
      return super.readDouble(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX double field", e);
    }
  }

  @Override
  public float readFloat(int pos) {
    try {
      return super.readFloat(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX float field", e);
    }
  }

  @Override
  public int readInt(int pos) {
    try {
      return super.readInt(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX int field", e);
    }
  }

  @Override
  public long readLong(int pos) {
    try {
      return super.readLong(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX long field", e);
    }
  }

  @Override
  public short readShort(int pos) {
    try {
      return super.readShort(pos);
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX short field", e);
    }
  }

  @Override
  public void position(int absPos) {
    try {
      super.position(absPos);
    } catch (IllegalArgumentException e) {
      throw new PdxSerializationException("Internal error; failed to set position to " + absPos, e);
    }
  }

  public String readString() {
    try {
      return DataSerializer.readString(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public Object readObject() {
    try {
      return DataSerializer.readObject(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    } catch (ClassNotFoundException e) {
      throw new PdxSerializationException("Class not found deserializing a PDX field", e);
    }
  }

  public char[] readCharArray() {
    try {
      return DataSerializer.readCharArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public boolean[] readBooleanArray() {
    try {
      return DataSerializer.readBooleanArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public byte[] readByteArray() {
    try {
      return DataSerializer.readByteArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public short[] readShortArray() {
    try {
      return DataSerializer.readShortArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public int[] readIntArray() {
    try {
      return DataSerializer.readIntArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public long[] readLongArray() {
    try {
      return DataSerializer.readLongArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public float[] readFloatArray() {
    try {
      return DataSerializer.readFloatArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public double[] readDoubleArray() {
    try {
      return DataSerializer.readDoubleArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public String[] readStringArray() {
    try {
      return DataSerializer.readStringArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    }
  }

  public Object[] readObjectArray() {
    try {
      return DataSerializer.readObjectArray(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    } catch (ClassNotFoundException e) {
      throw new PdxSerializationException("Class not found while deserializing a PDX field", e);
    }
  }

  public byte[][] readArrayOfByteArrays() {
    try {
      return DataSerializer.readArrayOfByteArrays(this);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception deserializing a PDX field", e);
    } catch (ClassNotFoundException ex) {
      throw new InternalGemFireException(
          "ClassNotFoundException should never be thrown but it was", ex);
    }
  }

  public Date readDate() {
    long time = readLong();
    return convertLongToDate(time);
  }

  @Override
  public boolean readBoolean() {
    try {
      return super.readBoolean();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX boolean field", e);
    }
  }

  @Override
  public byte readByte() {
    try {
      return super.readByte();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX byte field", e);
    }
  }

  @Override
  public char readChar() {
    try {
      return super.readChar();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX char field", e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return super.readDouble();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX double field", e);
    }
  }

  @Override
  public float readFloat() {
    try {
      return super.readFloat();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX float field", e);
    }
  }

  @Override
  public int readInt() {
    try {
      return super.readInt();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX int field", e);
    }
  }

  @Override
  public long readLong() {
    try {
      return super.readLong();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX long field", e);
    }
  }

  @Override
  public short readShort() {
    try {
      return super.readShort();
    } catch (IndexOutOfBoundsException e) {
      throw new PdxSerializationException("Failed reading a PDX short field", e);
    }
  }
  
  @Override
  public ByteSource slice(int startOffset, int endOffset) {
    try {
      return super.slice(startOffset, endOffset);
    } catch (IllegalArgumentException e) {
      throw new PdxSerializationException("Internal error; failed to slice start=" + startOffset + " end="+ endOffset, e);
    }
  }

  private Date convertLongToDate(long v) {
    Date result = null;
    if (v != -1L) {
      result = new Date(v);
    }
    return result;
  }
}
