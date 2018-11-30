/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.geode.internal.shared.unsafe;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.pdx.internal.unsafe.UnsafeWrapper;

@SuppressWarnings("serial")
public abstract class FreeMemory extends AtomicLong implements Runnable {

  protected FreeMemory(long address) {
    super(address);
  }

  protected final long tryFree() {
    // try hard to ensure freeMemory call happens only once
    final long address = get();
    return (address != 0 && compareAndSet(address, 0L)) ? address : 0L;
  }

  protected abstract String objectName();

  @Override
  public void run() {
    final long address = tryFree();
    if (address != 0) {
      new UnsafeWrapper().freeMemory(address);
    }
  }

  public interface Factory {
    FreeMemory newFreeMemory(long address, int size);
  }
}
