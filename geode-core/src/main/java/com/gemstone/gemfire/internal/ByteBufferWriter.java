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
package com.gemstone.gemfire.internal;

import java.nio.ByteBuffer;

/**
 * Used by a couple of our classes to say they can have
 * a ByteBuffer written to them. 
 */
public interface ByteBufferWriter {
  /**
   * Writes bb.position()..bb.limit() bytes to this writer.
   * Note that some implementations of this interface will
   * keep a reference to bb so callers should expect to give
   * up ownership of bb and should not modify it after calling
   * this method.
   */
  public void write(ByteBuffer bb);
}
