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
 * Basic size and usage information about an off-heap memory block under
 * inspection. For test validation only.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface MemoryBlock {

  public enum State {
    /** Unused fragment (not used and not in a free list) */
    UNUSED, 
    /** Allocated chunk currently in use */
    ALLOCATED, 
    /** Deallocated chunk currently in a free list */
    DEALLOCATED 
  }
  
  public State getState();
  
  /**
   * Returns the unsafe memory address of the first byte of this block.
   */
  public long getMemoryAddress();
  
  /**
   * Returns the size of this memory block in bytes.
   */
  public int getBlockSize();
  
  /**
   * Returns the next memory block immediately after this one.
   */
  public MemoryBlock getNextBlock();
  
  /**
   * Returns the identifier of which slab contains this block.
   */
  public int getSlabId();
  
  /**
   * Returns the identifier of which free list contains this block.
   */
  public int getFreeListId();
  
  public int getRefCount();
  public String getDataType();
  public boolean isSerialized();
  public boolean isCompressed();
  public Object getDataValue();
}
