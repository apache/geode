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
package org.apache.geode.pdx.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.geode.DataSerializer;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.internal.ByteBufferWriter;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;

/**
 * Used by PdxWriterImpl to manage the raw data of a PDX.
 *
 */
public class PdxOutputStream implements ByteBufferWriter {

  private final HeapDataOutputStream hdos;
  
  /**
   * 
   */
  public PdxOutputStream() {
    this.hdos = new HeapDataOutputStream(Version.CURRENT);
  }

  /**
   * @param allocSize
   */
  public PdxOutputStream(int allocSize) {
    this.hdos = new HeapDataOutputStream(allocSize, Version.CURRENT);
  }

  /**
   * Wrapper constructor
   * @param hdos
   */
  public PdxOutputStream(HeapDataOutputStream hdos) {
    this.hdos = hdos;
  }

  public void writeDate(Date date) {
    try {
      DataSerializer.writeDate(date, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeString(String value) {
    try {
      DataSerializer.writeString(value, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeObject(Object object, boolean ensureCompatibility) {
    try {
      InternalDataSerializer.basicWriteObject(object, this.hdos, ensureCompatibility);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeBooleanArray(boolean[] array) {
    try {
      DataSerializer.writeBooleanArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeCharArray(char[] array) {
    try {
      DataSerializer.writeCharArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeByteArray(byte[] array) {
    try {
      DataSerializer.writeByteArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeShortArray(short[] array) {
    try {
      DataSerializer.writeShortArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeIntArray(int[] array) {
    try {
      DataSerializer.writeIntArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeLongArray(long[] array) {
    try {
      DataSerializer.writeLongArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeFloatArray(float[] array) {
    try {
      DataSerializer.writeFloatArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeDoubleArray(double[] array) {
    try {
      DataSerializer.writeDoubleArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeStringArray(String[] array) {
    try {
      DataSerializer.writeStringArray(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeObjectArray(Object[] array, boolean ensureCompatibility) {
    try {
      InternalDataSerializer.writeObjectArray(array, this.hdos, ensureCompatibility);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeArrayOfByteArrays(byte[][] array) {
    try {
      DataSerializer.writeArrayOfByteArrays(array, this.hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public int size() {
    return this.hdos.size();
  }

  public void writeChar(char value) {
    this.hdos.writeChar(value);
  }

  public void writeByte(int value) {
    this.hdos.writeByte(value);
  }

  public void writeShort(short value) {
    this.hdos.writeShort(value);
  }

  public void writeInt(int value) {
    this.hdos.writeInt(value);
  }

  public void writeLong(long value) {
    this.hdos.writeLong(value);
  }

  public void writeFloat(float value) {
    this.hdos.writeFloat(value);
  }

  public void writeDouble(double value) {
    this.hdos.writeDouble(value);
  }

  public HeapDataOutputStream.LongUpdater reserveLong() {
    return this.hdos.reserveLong();
  }

  public void write(byte b) {
    this.hdos.write(b);
  }

  public void sendTo(DataOutput out) throws IOException {
    this.hdos.sendTo(out);
  }

  @Override
  public void write(ByteBuffer data) {
    this.hdos.write(data);
  }

  public void write(ByteSource data) {
    this.hdos.write(data);
  }

  public ByteBuffer toByteBuffer() {
    return this.hdos.toByteBuffer();
  }
  
  public byte[] toByteArray() {
    return this.hdos.toByteArray();
  }

  public void write(byte[] source, int offset, int len) {
    this.hdos.write(source, offset, len);
  }
}
