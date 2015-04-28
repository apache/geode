/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.CopyOnWriteHashSet;

/**
 * A singleton which keeps track of all of the dependency monitors registered in
 * this VM.
 * 
 * Dependency monitors track dependencies between threads that may not be known
 * to the JVM. For example, a thread in one VM may be waiting for a response to
 * a thread in another VM.
 * 
 * {@link DependencyMonitor}s should register themselves with this class. Then, the 
 * {@link DeadlockDetector} will be able to query them for dependencies when finding
 * deadlocks.
 * 
 * @author dsmith
 * 
 */
public class DependencyMonitorManager {
  
  private static Set<DependencyMonitor> monitors = new CopyOnWriteHashSet<DependencyMonitor>();
  
  static {
    //The DLockDependencyMonitor won't get loaded unless we add it here.
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
   * Get the set of all blocked threads and their dependencies in this VM, as reported
   * by the dependency monitors registered with this manager.
   */
  public static Set<Dependency<Thread, Serializable>> getBlockedThreads() {
    Set<Dependency<Thread, Serializable>> blockedThreads = new HashSet<Dependency<Thread, Serializable>>();
    Thread[] allThreads = getAllThreads();
    for(DependencyMonitor monitor : monitors) {
      blockedThreads.addAll(monitor.getBlockedThreads(allThreads));
    }
    
    return blockedThreads;
  }
  
  /**
   * Get the set of all resources which are held by threads in this VM, as reported
   * by the dependency monitors registered with this manager. 
   */
  public static Set<Dependency<Serializable, Thread>> getHeldResources() {
    Thread[] allThreads = getAllThreads();
    Set<Dependency<Serializable, Thread>> heldResources = new HashSet<Dependency<Serializable, Thread>>();
    for(DependencyMonitor monitor : monitors) {
      heldResources.addAll(monitor.getHeldResources(allThreads));
    }
    
    return heldResources;
  }
  
  /**
   * Get all of the threads in this VM.
   * TODO - do this more efficiently.
   * TODO - move this to a more appropriate location.
   */
  public static Thread[] getAllThreads() {
    
    //Ok, this lame. This seems to be the easiest way
    //to get all threads in java. Weak.
    Map<Thread, StackTraceElement[]> allStacks = Thread.getAllStackTraces();
    Thread[] results = new Thread[allStacks.size()];
    results = allStacks.keySet().toArray(results);
    return results;
  }

}
