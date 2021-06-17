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

package org.apache.geode.internal.net;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link AutoCloseable} meant to be acquired in a try-with-resources statement. The resource (a
 * {@link ByteBuffer}) is available (for reading and modification) in the scope of the
 * try-with-resources.
 *
 * This implementation is a "no-op". It performs no actual locking and no reference counting. It's
 * meant for use with the {@link NioPlainEngine} only, since that engine keeps no buffers and so,
 * needs no reference counting on buffers, nor any synchronization around access to buffers.
 *
 * See also {@link ByteBufferSharingImpl}
 */
class ByteBufferSharingNoOp implements ByteBufferSharing {

  private final ByteBuffer buffer;

  ByteBufferSharingNoOp(final ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public ByteBuffer getBuffer() {
    return buffer;
  }

  @Override
  public ByteBuffer expandWriteBufferIfNeeded(final int newCapacity) throws IOException {
    throw new UnsupportedOperationException("Can't expand buffer when using NioPlainEngine");
  }

  @Override
  public void close() {}
}
