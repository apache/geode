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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.geode.internal.tcp.ByteBufferInputStream;

/**
 * Provides byte stream for testing. Use {@link #getDataInput()} and {@link #getDataOutput()} to get
 * DataInput or DataOutput as needed for testing.
 *
 * @since GemFire 7.0
 */
public class ByteArrayData {

  private final ByteArrayOutputStream baos;

  public ByteArrayData() {
    baos = new ByteArrayOutputStream();
  }

  public int size() {
    return baos.size();
  }

  public boolean isEmpty() {
    return baos.size() == 0;
  }

  /**
   * Returns a <code>DataOutput</code> to write to
   */
  public DataOutputStream getDataOutput() {
    return new DataOutputStream(baos);
  }

  /**
   * Returns a <code>DataInput</code> to read from
   */
  public DataInput getDataInput() {
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    return bbis;
  }

  public DataInputStream getDataInputStream() {
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    return new DataInputStream(bbis);
  }

}
