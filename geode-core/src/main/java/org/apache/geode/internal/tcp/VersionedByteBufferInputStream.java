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

package org.apache.geode.internal.tcp;

import java.nio.ByteBuffer;

import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataStream;

/**
 * An extension to {@link ByteBufferInputStream} that implements {@link VersionedDataStream} for a
 * stream coming from a different product version.
 *
 * @since GemFire 7.1
 */
public class VersionedByteBufferInputStream extends ByteBufferInputStream
    implements VersionedDataStream {

  private final KnownVersion version;

  /**
   * Create a ByteBuffer input stream whose contents are null at given product {@link KnownVersion}.
   *
   * @param version the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(KnownVersion version) {
    super();
    this.version = version;
  }

  /**
   * Create a ByteBuffer input stream whose contents are the given {@link ByteBuffer} at given
   * product {@link KnownVersion}.
   *
   * @param buffer the byte buffer to read
   * @param version the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(ByteBuffer buffer, KnownVersion version) {
    super(buffer);
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
  public String toString() {
    return super.toString() + " (" + version + ')';
  }
}
