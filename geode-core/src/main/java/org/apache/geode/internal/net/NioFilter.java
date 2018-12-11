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
 * Prior to transmitting a buffer or processing a received buffer
 * a NioFilter should be called to wrap (transmit) or unwrap (received)
 * the buffer in case SSL is being used.
 */
public interface NioFilter {

  /** wrap bytes for transmission to another process */
  ByteBuffer wrap(ByteBuffer buffer) throws IOException;

  /** unwrap bytes received from another process */
  ByteBuffer unwrap(ByteBuffer wrappedBuffer) throws IOException;

  /**
   * You must invoke this when done reading from the unwrapped buffer
   */
  default void doneReading(ByteBuffer unwrappedBuffer) {
    if (unwrappedBuffer.position() != 0) {
      unwrappedBuffer.compact();
    } else {
      unwrappedBuffer.position(unwrappedBuffer.limit());
      unwrappedBuffer.limit(unwrappedBuffer.capacity());
    }
  }

  /** invoke this method when you are done using the NioFilter */
  void close();

}
