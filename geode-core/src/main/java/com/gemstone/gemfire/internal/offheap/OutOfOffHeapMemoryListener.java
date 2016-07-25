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

import com.gemstone.gemfire.OutOfOffHeapMemoryException;

/**
 * Listens to the MemoryAllocator for notification of OutOfOffHeapMemoryError.
 * 
 * The implementation created by OffHeapStorage for a real DistribytedSystem
 * connection causes the System and Cache to close in order to avoid data
 * inconsistency.
 * 
 * @since Geode 1.0
 */
public interface OutOfOffHeapMemoryListener {

  /**
   * Notification that an OutOfOffHeapMemoryError has occurred.
   * 
   * @param cause the actual OutOfOffHeapMemoryError that was thrown
   */
  public void outOfOffHeapMemory(OutOfOffHeapMemoryException cause);
  
  /**
   * Close any resources used by this listener.
   */
  public void close();
}
