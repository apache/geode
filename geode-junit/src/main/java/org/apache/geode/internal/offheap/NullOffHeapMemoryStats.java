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
 * Null implementation of OffHeapMemoryStats for testing.
 *
 */
public class NullOffHeapMemoryStats implements OffHeapMemoryStats {
  private boolean isClosed;

  public void incFreeMemory(long value) {}

  public void incMaxMemory(long value) {}

  public void incUsedMemory(long value) {}

  public void incSlabSize(long value) {}

  public void incObjects(int value) {}

  public long getFreeMemory() {
    return 0;
  }

  public long getMaxMemory() {
    return 0;
  }

  public long getUsedMemory() {
    return 0;
  }

  public long getSlabSize() {
    return 0;
  }

  public int getObjects() {
    return 0;
  }

  @Override
  public void incReads() {}

  @Override
  public long getReads() {
    return 0;
  }

  @Override
  public int getDefragmentations() {
    return 0;
  }

  @Override
  public int getDefragmentationsInProgress() {
    return 0;
  }

  @Override
  public void setFragments(long value) {}

  @Override
  public long getFragments() {
    return 0;
  }

  @Override
  public void setLargestFragment(int value) {}

  @Override
  public int getLargestFragment() {
    return 0;
  }

  @Override
  public long startDefragmentation() {
    return 0;
  }

  @Override
  public void endDefragmentation(long start) {}

  @Override
  public void setFragmentation(int value) {}

  @Override
  public int getFragmentation() {
    return 0;
  }

  @Override
  public Statistics getStats() {
    return null;
  }

  @Override
  public long getDefragmentationTime() {
    return 0;
  }

  @Override
  public void close() {
    this.isClosed = true;
  }

  @Override
  public void initialize(OffHeapMemoryStats stats) {
    stats.close();
  }

  public boolean isClosed() {
    return this.isClosed;
  }
}
