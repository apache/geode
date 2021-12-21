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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * Used by tests to get notifications about the lifecycle of a MemoryAllocatorImpl.
 *
 */
public interface LifecycleListener {

  /**
   * Callback is invoked after creating a new MemoryAllocatorImpl.
   *
   * Create occurs during the first initialization of an InternalDistributedSystem within the JVM.
   *
   * @param allocator the instance that has just been created
   */
  void afterCreate(MemoryAllocatorImpl allocator);

  /**
   * Callback is invoked after reopening an existing MemoryAllocatorImpl for reuse.
   *
   * Reuse occurs during any intialization of an InternalDistributedSystem after the first one was
   * connected and then disconnected within the JVM.
   *
   * @param allocator the instance that has just been reopened for reuse
   */
  void afterReuse(MemoryAllocatorImpl allocator);

  /**
   * Callback is invoked before closing the MemoryAllocatorImpl
   *
   * Close occurs after the InternalDistributedSystem and DistributionManager have completely
   * disconnected.
   *
   * @param allocator the instance that is about to be closed
   */
  void beforeClose(MemoryAllocatorImpl allocator);

  static void invokeBeforeClose(MemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.beforeClose(allocator);
    }
  }

  static void invokeAfterReuse(MemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.afterReuse(allocator);
    }
  }

  static void invokeAfterCreate(MemoryAllocatorImpl allocator) {
    for (Iterator<LifecycleListener> iter = lifecycleListeners.iterator(); iter.hasNext();) {
      LifecycleListener listener = iter.next();
      listener.afterCreate(allocator);
    }
  }

  /**
   * Removes a LifecycleListener. Does nothing if the instance has not been added.
   *
   * @param listener the instance to remove
   */
  static void removeLifecycleListener(LifecycleListener listener) {
    lifecycleListeners.remove(listener);
  }

  /**
   * Adds a LifecycleListener.
   *
   * @param listener the instance to add
   */
  static void addLifecycleListener(LifecycleListener listener) {
    LifecycleListener.lifecycleListeners.add(listener);
  }

  /**
   * Following should be private but java 8 does not support that.
   */
  @MakeNotStatic
  List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();
}
