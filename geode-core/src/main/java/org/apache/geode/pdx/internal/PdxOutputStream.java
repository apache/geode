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

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.ByteBufferWriter;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;
import org.apache.geode.pdx.PdxSerializationException;

/**
 * Used by PdxWriterImpl to manage the raw data of a PDX.
 *
 */
public class PdxOutputStream implements ByteBufferWriter {

  private final HeapDataOutputStream hdos;

  public PdxOutputStream() {
    hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
  }

  public PdxOutputStream(int allocSize) {
    hdos = new HeapDataOutputStream(allocSize, KnownVersion.CURRENT);
  }

  /**
   * Wrapper constructor
   *
   */
  public PdxOutputStream(HeapDataOutputStream hdos) {
    this.hdos = hdos;
  }

  public void writeDate(Date date) {
    try {
      DataSerializer.writeDate(date, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeString(String value) {
    try {
      DataSerializer.writeString(value, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeObject(Object object, boolean ensureCompatibility) {
    try {
      InternalDataSerializer.basicWriteObject(object, hdos, ensureCompatibility);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeBooleanArray(boolean[] array) {
    try {
      DataSerializer.writeBooleanArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeCharArray(char[] array) {
    try {
      DataSerializer.writeCharArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeByteArray(byte[] array) {
    try {
      DataSerializer.writeByteArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeShortArray(short[] array) {
    try {
      DataSerializer.writeShortArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeIntArray(int[] array) {
    try {
      DataSerializer.writeIntArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeLongArray(long[] array) {
    try {
      DataSerializer.writeLongArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeFloatArray(float[] array) {
    try {
      DataSerializer.writeFloatArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeDoubleArray(double[] array) {
    try {
      DataSerializer.writeDoubleArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeStringArray(String[] array) {
    try {
      DataSerializer.writeStringArray(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeObjectArray(Object[] array, boolean ensureCompatibility) {
    try {
      InternalDataSerializer.writeObjectArray(array, hdos, ensureCompatibility);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public void writeArrayOfByteArrays(byte[][] array) {
    try {
      DataSerializer.writeArrayOfByteArrays(array, hdos);
    } catch (IOException e) {
      throw new PdxSerializationException("Exception while serializing a PDX field", e);
    }
  }

  public int size() {
    return hdos.size();
  }

  public void writeChar(char value) {
    hdos.writeChar(value);
  }

  public void writeByte(int value) {
    hdos.writeByte(value);
  }

  public void writeShort(short value) {
    hdos.writeShort(value);
  }

  public void writeInt(int value) {
    hdos.writeInt(value);
  }

  public void writeLong(long value) {
    hdos.writeLong(value);
  }

  public void writeFloat(float value) {
    hdos.writeFloat(value);
  }

  public void writeDouble(double value) {
    hdos.writeDouble(value);
  }

  public HeapDataOutputStream.LongUpdater reserveLong() {
    return hdos.reserveLong();
  }

  public void write(byte b) {
    hdos.write(b);
  }

  public void sendTo(DataOutput out) throws IOException {
    hdos.sendTo(out);
  }

  @Override
  public void write(ByteBuffer data) {
    hdos.write(data);
  }

  public void write(ByteSource data) {
    hdos.write(data);
  }

  public ByteBuffer toByteBuffer() {
    return hdos.toByteBuffer();
  }

  public ByteBuffer toByteBuffer(int startPosition) {
    return hdos.toByteBuffer(startPosition);
  }

  public byte[] toByteArray() {
    return hdos.toByteArray();
  }

  public void write(byte[] source, int offset, int len) {
    hdos.write(source, offset, len);
  }
}
