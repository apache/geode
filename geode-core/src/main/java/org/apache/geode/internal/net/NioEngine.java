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

import java.nio.ByteBuffer;

import org.apache.geode.distributed.internal.DMStats;

/**
 * A pass-through implementation of NioFilter. Use this if you don't need
 * secure communications.
 */
public class NioEngine implements NioFilter {

  public NioEngine() {}

  @Override
  public ByteBuffer wrap(ByteBuffer buffer) {
    return buffer;
  }

  @Override
  public ByteBuffer unwrap(ByteBuffer wrappedBuffer) {
    return wrappedBuffer;
  }

  @Override
  public void close() {}

  @Override
  public ByteBuffer getUnwrappedBuffer(ByteBuffer wrappedBuffer) {
    return wrappedBuffer;
  }

  @Override
  public ByteBuffer ensureUnwrappedCapacity(int amount, ByteBuffer wrappedBuffer,
      Buffers.BufferType bufferType,
      DMStats stats) {
    return Buffers.expandBuffer(bufferType, wrappedBuffer, amount, stats);
  }

}
