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

import org.apache.geode.pdx.PdxSerializationException;

/**
 * Used by {@link PdxInstanceImpl} as its input stream.
 * Fixed width fields on this implementation do not change the position
 * but instead use absolute positions.
 * 
 * @since GemFire 6.6.2
 */
public class PdxInstanceInputStream extends PdxInputStream {


  public PdxInstanceInputStream(PdxInputStream in, int len) {
    super(in, len);
  }

  public PdxInstanceInputStream(byte[] bytes) {
    super(bytes);
  }
  
  public PdxInstanceInputStream() {
    // for serialization
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
}
