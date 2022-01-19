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
import java.io.ObjectOutput;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataStream;

/**
 * An extension to {@link ObjectOutput} that implements {@link VersionedDataStream} wrapping given
 * {@link ObjectOutput} for a stream directed to a different product version.
 *
 * @since GemFire 7.1
 */
public class VersionedObjectOutput implements ObjectOutput, VersionedDataStream {

  private final ObjectOutput out;
  private final KnownVersion version;

  /**
   * Creates a VersionedObjectOutput that wraps the specified underlying ObjectOutput.
   *
   * @param out the underlying {@link ObjectOutput}
   * @param version the product version that serialized object on the given {@link ObjectOutput}
   */
  public VersionedObjectOutput(ObjectOutput out, KnownVersion version) {
    if (version.isNewerThan(KnownVersion.CURRENT)) {
      Assert.fail("unexpected version: " + version + ", CURRENT: " + KnownVersion.CURRENT);
    }
    this.out = out;
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
  public void write(int b) throws IOException {
    out.write(b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {
    out.writeBoolean(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeByte(int v) throws IOException {
    out.writeByte(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeShort(int v) throws IOException {
    out.writeShort(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeChar(int v) throws IOException {
    out.writeChar(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeInt(int v) throws IOException {
    out.writeInt(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeLong(long v) throws IOException {
    out.writeLong(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeFloat(float v) throws IOException {
    out.writeFloat(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeDouble(double v) throws IOException {
    out.writeDouble(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeBytes(String s) throws IOException {
    out.writeBytes(s);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeChars(String s) throws IOException {
    out.writeChars(s);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeUTF(String s) throws IOException {
    out.writeUTF(s);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeObject(Object obj) throws IOException {
    out.writeObject(obj);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    out.flush();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    out.close();
  }
}
