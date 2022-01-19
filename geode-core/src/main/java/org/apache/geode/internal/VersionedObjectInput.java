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

package org.apache.geode.internal;

import java.io.IOException;
import java.io.ObjectInput;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataStream;

/**
 * An extension to {@link ObjectInput} that implements {@link VersionedDataStream} wrapping given
 * {@link ObjectInput} for a stream coming from a different product version.
 *
 * @since GemFire 7.1
 */
public class VersionedObjectInput implements ObjectInput, VersionedDataStream {

  private final ObjectInput in;
  private final KnownVersion version;

  /**
   * Creates a VersionedObjectInput that wraps the specified underlying ObjectInput.
   *
   * @param in the specified {@link ObjectInput}
   * @param version the product version that serialized object on the given {@link ObjectInput}
   */
  public VersionedObjectInput(ObjectInput in, KnownVersion version) {
    this.in = in;
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KnownVersion getVersion() {
    return version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(byte[] b) throws IOException {
    in.readFully(b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    in.readFully(b, off, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int skipBytes(int n) throws IOException {
    return in.skipBytes(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean readBoolean() throws IOException {
    return in.readBoolean();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readUnsignedByte() throws IOException {
    return in.readUnsignedByte();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public short readShort() throws IOException {
    return in.readShort();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readUnsignedShort() throws IOException {
    return in.readUnsignedShort();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public char readChar() throws IOException {
    return in.readChar();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readInt() throws IOException {
    return in.readInt();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long readLong() throws IOException {
    return in.readLong();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float readFloat() throws IOException {
    return in.readFloat();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double readDouble() throws IOException {
    return in.readDouble();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLine() throws IOException {
    return in.readLine();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readUTF() throws IOException {
    return in.readUTF();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object readObject() throws ClassNotFoundException, IOException {
    return in.readObject();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    return in.read();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long skip(long n) throws IOException {
    return in.skip(n);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int available() throws IOException {
    return in.available();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    in.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "VersionedObjectInput@" + Integer.toHexString(System.identityHashCode(this)) + " ("
        + version + ')';
  }
}
