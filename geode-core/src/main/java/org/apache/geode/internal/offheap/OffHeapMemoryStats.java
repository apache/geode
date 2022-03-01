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
package org.apache.geode.internal.offheap;

import org.apache.geode.Statistics;

/**
 * Statistics for off-heap memory storage.
 *
 * @since Geode 1.0
 */
public interface OffHeapMemoryStats {

  void incFreeMemory(long value);

  void incMaxMemory(long value);

  void incUsedMemory(long value);

  void incObjects(int value);

  void incReads();

  void setFragments(long value);

  void setLargestFragment(int value);

  long startDefragmentation();

  void endDefragmentation(long start);

  void setFragmentation(int value);

  void setFreedChunks(int value);

  long getFreeMemory();

  long getMaxMemory();

  long getUsedMemory();

  long getReads();

  int getObjects();

  int getDefragmentations();

  int getDefragmentationsInProgress();

  long getFragments();

  long getFreedChunks();

  int getLargestFragment();

  int getFragmentation();

  long getDefragmentationTime();

  Statistics getStats();

  void close();

  void initialize(OffHeapMemoryStats stats);
}
