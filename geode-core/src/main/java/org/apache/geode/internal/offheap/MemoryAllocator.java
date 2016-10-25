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
package org.apache.geode.internal.offheap;

/**
 * Basic contract for a heap that manages off heap memory. Any MemoryChunks allocated from a heap
 * are returned to that heap when freed.
 * 
 * @since Geode 1.0
 */
public interface MemoryAllocator {
  /**
   * @param size the size in bytes of the chunk of memory to allocate
   * @return the allocated chunk of memory.
   * @throws IllegalStateException if the heap does not have enough memory to grant the request
   */
  public StoredObject allocate(int size);
  
  /**
   * Allocates off heap memory for the given data and returns a StoredObject
   * that is backed by this allocated memory and that contains the data.
   * @param data the bytes of the data to put in the allocated StoredObject
   * @param isSerialized true if data contains a serialized object; false if it is an actual byte array.
   * @param isCompressed true if data is compressed; false if it is uncompressed.
   * @throws IllegalStateException if the heap does not have enough memory to grant the request
   */
  public StoredObject allocateAndInitialize(byte[] data, boolean isSerialized, boolean isCompressed);
  
  /**
   * Allocates off heap memory for the given data and returns a StoredObject
   * that is backed by this allocated memory and that contains the data
   * and keeps a reference to the original heap data.
   * @param data the bytes of the data to put in the allocated StoredObject
   * @param isSerialized true if data contains a serialized object; false if it is an actual byte array.
   * @param originalHeapData the original uncompressed heap data
   * @param isCompressed true if data is compressed; false if it is uncompressed.
   * @throws IllegalStateException if the heap does not have enough memory to grant the request
   */
  public StoredObject allocateAndInitialize(byte[] data, boolean isSerialized, boolean isCompressed, byte[] originalHeapData);
  
  public long getFreeMemory();
  
  public long getUsedMemory();

  public long getTotalMemory();

  public OffHeapMemoryStats getStats();

  /**
   * This allocator will no longer be used so free up any system memory that belongs to it.
   */
  public void close();

  public MemoryInspector getMemoryInspector();
  
  public void addMemoryUsageListener(MemoryUsageListener listener);
  
  public void removeMemoryUsageListener(MemoryUsageListener listener);
}
