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
 * A "slab" of memory.
 * Slabs can be created by calling {@link AddressableMemoryManager#allocateSlab(int)}.
 * Slabs have an address, a size, and can be freed.
 */
public interface Slab {
  /**
   * Return the address of the memory of this slab.
   */
  public long getMemoryAddress();
  /**
   * Returns the size of this memory chunk in bytes.
   */
  public int getSize();
  /**
   * Returns any memory allocated for this slab.
   * Note that after free is called the address of
   * this slab should no longer be used.
   */
  public void free();
}