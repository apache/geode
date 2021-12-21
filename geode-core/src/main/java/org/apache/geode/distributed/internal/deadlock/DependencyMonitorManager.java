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
package org.apache.geode.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.CopyOnWriteHashSet;

/**
 * A singleton which keeps track of all of the dependency monitors registered in this VM.
 *
 * Dependency monitors track dependencies between threads that may not be known to the JVM. For
 * example, a thread in one VM may be waiting for a response to a thread in another VM.
 *
 * {@link DependencyMonitor}s should register themselves with this class. Then, the
 * {@link DeadlockDetector} will be able to query them for dependencies when finding deadlocks.
 *
 *
 */
public class DependencyMonitorManager {

  @MakeNotStatic
  private static final Set<DependencyMonitor> monitors =
      new CopyOnWriteHashSet<>();

  static {
    // The DLockDependencyMonitor won't get loaded unless we add it here.
    addMonitor(DLockDependencyMonitor.INSTANCE);
  }

  /**
   * Register a dependency monitor.
   */
  public static void addMonitor(DependencyMonitor monitor) {
    monitors.add(monitor);
  }

  /**
   * Unregister a dependency monitor.
   */
  public static void removeMonitor(DependencyMonitor monitor) {
    monitors.remove(monitor);
  }

  /**
   * Get the set of all blocked threads and their dependencies in this VM, as reported by the
   * dependency monitors registered with this manager.
   */
  public static Set<Dependency<Thread, Serializable>> getBlockedThreads() {
    Set<Dependency<Thread, Serializable>> blockedThreads =
        new HashSet<>();
    Thread[] allThreads = getAllThreads();
    for (DependencyMonitor monitor : monitors) {
      blockedThreads.addAll(monitor.getBlockedThreads(allThreads));
    }

    return blockedThreads;
  }

  /**
   * Get the set of all resources which are held by threads in this VM, as reported by the
   * dependency monitors registered with this manager.
   */
  public static Set<Dependency<Serializable, Thread>> getHeldResources() {
    Thread[] allThreads = getAllThreads();
    Set<Dependency<Serializable, Thread>> heldResources =
        new HashSet<>();
    for (DependencyMonitor monitor : monitors) {
      heldResources.addAll(monitor.getHeldResources(allThreads));
    }

    return heldResources;
  }

  /**
   * Get all of the threads in this VM. TODO - do this more efficiently. TODO - move this to a more
   * appropriate location.
   */
  public static Thread[] getAllThreads() {

    // Ok, this lame. This seems to be the easiest way
    // to get all threads in java. Weak.
    Map<Thread, StackTraceElement[]> allStacks = Thread.getAllStackTraces();
    Thread[] results = new Thread[allStacks.size()];
    results = allStacks.keySet().toArray(results);
    return results;
  }

}
