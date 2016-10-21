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

  public void incFreeMemory(long value);

  public void incMaxMemory(long value);

  public void incUsedMemory(long value);

  public void incObjects(int value);

  public void incReads();

  public void setFragments(long value);

  public void setLargestFragment(int value);

  public long startDefragmentation();

  public void endDefragmentation(long start);

  public void setFragmentation(int value);

  public long getFreeMemory();

  public long getMaxMemory();

  public long getUsedMemory();

  public long getReads();

  public int getObjects();

  public int getDefragmentations();

  public int getDefragmentationsInProgress();

  public long getFragments();

  public int getLargestFragment();

  public int getFragmentation();

  public long getDefragmentationTime();

  public Statistics getStats();

  public void close();

  public void initialize(OffHeapMemoryStats stats);
}
