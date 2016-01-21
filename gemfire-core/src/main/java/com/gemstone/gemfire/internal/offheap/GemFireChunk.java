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
package com.gemstone.gemfire.internal.offheap;


/**
 * A chunk that stores a GemFire object.
 * Currently the object stored in this chunk
 * is always an entry value of a Region.
 */
public class GemFireChunk extends Chunk {
  public static final ChunkType TYPE = new ChunkType() {
    @Override
    public int getSrcType() {
      return Chunk.SRC_TYPE_GFE;
    }
  };
  public GemFireChunk(long memoryAddress, int chunkSize) {
    super(memoryAddress, chunkSize, TYPE);
  }

  public GemFireChunk(long memoryAddress) {
    super(memoryAddress);
    // chunkType may be set by caller when it calls readyForAllocation
  }
  public GemFireChunk(GemFireChunk chunk) {
    super(chunk);
  }
  @Override
  public Chunk slice(int position, int limit) {
    return new GemFireChunkSlice(this, position, limit);
  }
}