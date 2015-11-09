/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that supports seeking to a particular position.
 */
public abstract class SeekableInputStream extends InputStream {
  
  /**
   * Seek to a position in the stream. The position is relative to the beginning
   * of the stream (in other words, just before the first byte that was ever
   * read).
   * 
   * @param position
   * @throws IOException if the seek goes past the end of the stream
   */
  public abstract void seek(long position) throws IOException;
  
  public abstract SeekableInputStream clone();


}
