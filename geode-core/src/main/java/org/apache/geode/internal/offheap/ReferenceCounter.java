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

import org.apache.geode.annotations.Immutable;

class ReferenceCounter {

  @Immutable
  private static final ReferenceCounter INSTANCE =
      new ReferenceCounter(new ReferenceCounterInstance());

  private final ReferenceCounterInstance delegate;

  private static ReferenceCounterInstance delegate() {
    return INSTANCE.delegate;
  }

  private ReferenceCounter(ReferenceCounterInstance delegate) {
    this.delegate = delegate;
  }

  static int getRefCount(long memAddr) {
    return delegate().getRefCount(memAddr);
  }

  static boolean retain(long memAddr) {
    return delegate().retain(memAddr);
  }

  static void release(final long memAddr) {
    delegate().release(memAddr);
  }

  static void release(final long memAddr, FreeListManager freeListManager) {
    delegate().release(memAddr, freeListManager);
  }
}
