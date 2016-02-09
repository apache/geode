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
 * ChunkFactory can be used to create Chunk instances.
 * It can also be used to determine the ChunkType given a Chunk address
 * or the object header bits from an existing Chunk.
 */
public interface ChunkFactory  {
  /**
   * Create a new chunk of the given size and type at the given address.
   */
  Chunk newChunk(long address, int chunkSize, ChunkType chunkType);
  /**
   * Create a new chunk for a block of memory (identified by address)
   * that has already been allocated.
   * The size and type are derived from the existing object header.
   */
  Chunk newChunk(long address);
  /**
   * Create a new chunk of the given type for a block of memory (identified by address)
   * that has already been allocated.
   * The size is derived from the existing object header.
   */
  Chunk newChunk(long address, ChunkType chunkType);
  /**
   * Given the address of an existing chunk return its ChunkType.
   */
  ChunkType getChunkTypeForAddress(long address);
  /**
   * Given the rawBits from the object header of an existing chunk
   * return its ChunkType.
   */
  ChunkType getChunkTypeForRawBits(int bits);
}