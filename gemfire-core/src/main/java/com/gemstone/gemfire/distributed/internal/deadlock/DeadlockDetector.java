/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * A class used for detecting deadlocks. The static method
 * {@link #collectAllDependencies(Serializable)} will find all dependencies
 * between threads and locks in the current VM.
 * 
 * To use this class, collect dependencies in each VM you want to analyze, and
 * add them to an instance of a {@link DeadlockDetector} using the
 * {@link #addDependencies(Set)} method. The {@link #findDeadlock()} method will
 * analyze the dependencies to find any deadlocks.
 * 
 * This class uses Java 1.6 management APIs on {@link ThreadMXBean}, so it will
 * not find any deadlocks in 1.5 VM. It also uses the
 * {@link DependencyMonitorManager} framework to collect dependencies that are
 * not reported by the VM - such as the association between message senders and
 * processors in different VMs.
 * 
 * @author dsmith
 * 
 */
public class DeadlockDetector {
  DependencyGraph graph = new DependencyGraph();

  public DeadlockDetector() {

  }

  /**
   * Add a set of dependencies to the dependency graph to
   * be analyzed.
   */
  public void addDependencies(Set<Dependency> dependencies) {
    for (Dependency dep : dependencies) {
      graph.addEdge(dep);
    }
  }

  /**
   * Finds the first deadlock in the list of dependencies, or null if there are
   * no deadlocks in the set of dependencies.
   * 
   * @return a linked list of dependencies which shows the circular
   *         dependencies. The List will be of the form Dependency(A,B),
   *         Dependency(B,C), Dependency(C, A).
   */
  public LinkedList<Dependency> findDeadlock() {
    return graph.findCycle();
  }

  /**
   * Get the dependency graph.
   */
  public DependencyGraph getDependencyGraph() {
    return graph;
  }

  /**
   * Find the all of the dependencies of a given thread.
   */
  public DependencyGraph findDependencyGraph(ThreadReference thread) {
    return graph.getSubGraph(thread);
  }

  /**
   * Collect all of the dependencies that exist between threads in this VM,
   * using java management beans and the {@link DependencyMonitor}.
   * 
   * Threads may depend on locks, or on other resources that are tracked by the
   * {@link DependencyMonitor}.
   * 
   * @return All of the dependencies between threads an locks or other resources
   *         on this VM.
   */
  public static Set<Dependency> collectAllDependencies(Serializable locality) {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(true, true);

    Set<Dependency> results = new HashSet<Dependency>();

    Map<Long, ThreadInfo> threadInfos = new HashMap<Long, ThreadInfo>();
    for (ThreadInfo info : infos) {
      // This can happen if the thread died.
      if (info == null) {
        continue;
      }

      for (LockInfo monitor : info.getLockedMonitors()) {
        Dependency dependency = new Dependency(new LocalLockInfo(locality,
            monitor), new LocalThread(locality, info));
        results.add(dependency);
      }

      for (LockInfo sync : info.getLockedSynchronizers()) {
        Dependency dependency = new Dependency(
            new LocalLockInfo(locality, sync), new LocalThread(locality, info));
        results.add(dependency);
      }

      LockInfo waitingFor = info.getLockInfo();
      if (waitingFor != null) {
        Dependency dependency = new Dependency(new LocalThread(locality, info),
            new LocalLockInfo(locality, waitingFor));
        results.add(dependency);
      }

      threadInfos.put(info.getThreadId(), info);
    }

    Set<Dependency> monitoredDependencies = collectFromDependencyMonitor(bean,
        locality, threadInfos);
    results.addAll(monitoredDependencies);
    return results;
  }
  
  /**
   * Format deadlock displaying to a user.
   */
  public static String prettyFormat(Collection<Dependency> deadlock) {
    StringBuilder text = new StringBuilder();
    LinkedHashSet<LocalThread> threads = new LinkedHashSet<LocalThread>();
    for (Dependency dep : deadlock) {
      Object depender = dep.getDepender();
      Object dependsOn = dep.getDependsOn();
      if (depender instanceof LocalThread) {
        text.append(depender + " is waiting on " + dependsOn + "\n");
        threads.add((LocalThread) depender);
      } else if (dependsOn instanceof LocalThread) {
        text.append(depender + " is held by " + dependsOn + "\n");
        threads.add((LocalThread) dependsOn);
      } else {
        text.append(depender + " is waiting for " + dependsOn + "\n");
      }
    }

    text.append("\nStack traces for involved threads\n");
    for (LocalThread threadInfo : threads) {
      text.append(
          threadInfo.getLocatility() + ":" + threadInfo.getThreadStack())
          .append("\n\n");
    }

    return text.toString();
  }

  /**
   * Format dependency graph for displaying to a user.
   */
  public static String prettyFormat(DependencyGraph graph) {
    return prettyFormat(graph.getEdges());
  }

  /**
   * Get an object suitable for querying the findDependencies method for a given
   * thread.
   */
  public static ThreadReference getThreadReference(String locality,
      Thread thread) {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo info = bean.getThreadInfo(thread.getId(), Integer.MAX_VALUE);
    return new LocalThread(locality, info);
  }
  
  private static Set<Dependency> collectFromDependencyMonitor(
      ThreadMXBean bean, Serializable locality,
      Map<Long, ThreadInfo> threadInfos) {
    HashSet<Dependency> results = new HashSet<Dependency>();

    // Convert the held resources into serializable dependencies
    Set<Dependency<Serializable, Thread>> heldResources = DependencyMonitorManager
        .getHeldResources();
    for (Dependency<Serializable, Thread> dep : heldResources) {
      Thread thread = dep.getDependsOn();
      Serializable resource = dep.getDepender();
      ThreadInfo info = threadInfos.get(thread.getId());
      if (info == null) {
        info = bean.getThreadInfo(thread.getId());
      }
      if(info != null) {
        results.add(new Dependency(resource, new LocalThread(locality, info)));
      }
    }
    
    Set<Dependency<Thread, Serializable>> blockedThreads = DependencyMonitorManager
    .getBlockedThreads();

    // Convert the blocked threads into serializable dependencies
    for (Dependency<Thread, Serializable> dep : blockedThreads) {
      Thread thread = dep.getDepender();
      ThreadInfo info = threadInfos.get(thread.getId());
      if (info == null) {
        info = bean.getThreadInfo(thread.getId());
      }
      final Serializable resource = dep.getDependsOn();
      results.add(new Dependency(new LocalThread(locality, info), resource));
    }
    return results;
  }
}
