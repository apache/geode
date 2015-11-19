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
 * A ChunkFactory that produces chunks of type GemFireChunk.
 */
public class GemFireChunkFactory implements ChunkFactory {
  @Override
  public Chunk newChunk(long address, int chunkSize, ChunkType chunkType) {
    assert chunkType.equals(GemFireChunk.TYPE);
    return new GemFireChunk(address,chunkSize);
  }

  @Override
  public Chunk newChunk(long address) {
    return new GemFireChunk(address);
  }

  @Override
  public Chunk newChunk(long address, ChunkType chunkType) {
    assert chunkType.equals(GemFireChunk.TYPE);
    return new GemFireChunk(address);
  }

  @Override
  public ChunkType getChunkTypeForAddress(long address) {
    assert Chunk.getSrcType(address) == Chunk.SRC_TYPE_GFE;
    return GemFireChunk.TYPE;
  }

  @Override
  public ChunkType getChunkTypeForRawBits(int bits) {
    assert Chunk.getSrcTypeFromRawBits(bits) == Chunk.SRC_TYPE_GFE;
    return GemFireChunk.TYPE;
  }
}