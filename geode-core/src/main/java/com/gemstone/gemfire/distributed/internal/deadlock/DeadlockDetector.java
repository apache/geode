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
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
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
import java.util.List;
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
 * This class also has a main() method that can read serialized DependencyGraphs
 * from multiple JVMs, merge them and perform various analysis on them.
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
    graph.addEdges(dependencies);
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
   * @param locality a name tag to stick on entities to help associate them with
   * this JVM and distinguish them from entities from other jvms
   * 
   * @return All of the dependencies between threads and locks or other resources
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
    
    Set<Object> seenDependers = new HashSet<>();
    Object lastDependsOn = text;
    Object lastDepender = text;
    
    for (Dependency dep : deadlock) {
      Object depender = dep.getDepender();
      Object dependsOn = dep.getDependsOn();
      
      String dependerString;
      if (lastDependsOn.equals(depender)) {
        dependerString = "which";
      } else if (lastDepender.equals(depender)){
        dependerString = "and";
      } else {
        dependerString = String.valueOf(depender);
      }
      lastDepender = depender;
      lastDependsOn = dependsOn;
      
      String also = seenDependers.contains(depender)? " also" : "";
      seenDependers.add(depender);
      
      if (depender instanceof LocalThread) {
        text.append(dependerString).append(" is").append(also).append(" waiting on ").append(dependsOn).append("\n");
        threads.add((LocalThread) depender);
      } else if (dependsOn instanceof LocalThread) {
        text.append(dependerString).append(" is held by thread ").append(dependsOn).append("\n");
        threads.add((LocalThread) dependsOn);
      } else {
        text.append(dependerString).append(" is").append(also).append(" waiting for ").append(dependsOn).append("\n");
      }
      text.append("\n");
    }

    text.append("\nStack traces for involved threads\n");
    for (LocalThread threadInfo : threads) {
      text.append(threadInfo.getLocatility())
          .append(":")
          .append(threadInfo.getThreadStack())
          .append("\n\n");
    }

    return text.toString();
  }
  
  
  /**
   * attempts to sort the given dependencies according to their contents
   * so that dependents come after dependers.
   * @param dependencies
   * TODO this method needs more work
   */
  public static List<Dependency> sortDependencies(Collection<Dependency> dependencies) {
    List<Dependency> result = new LinkedList<>();
    for (Dependency dep: dependencies) {
      boolean added = false;
      for (int i=0; i<result.size(); i++) {
        Dependency other = result.get(i);
        if (other.depender.equals(dep.depender)) {
          result.add(i, dep);
          added = true;
          break;
        }
        if (other.depender.equals(dep.dependsOn)) {
          result.add(i, dep);
          added = true;
          break;
        }
      }
      if (!added) {
        result.add(dep);
      }
    }
    return result;
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
  
  private static DependencyGraph loadGraphs(int startingAt, String... mainArgs) throws Exception {
    String filename;
    if (mainArgs.length < startingAt+1) {
      return loadGraph("thread_dependency_graph.ser");
    }
    
    DependencyGraph result = new DependencyGraph();
    
    for (int i=startingAt; i<mainArgs.length; i++) {
      filename = mainArgs[i];
      DependencyGraph gr = loadGraph(filename);
      if (gr == null) {
        return null;
      }
      result.addEdges(gr.getEdges());
    }
    
    return result;
  }
  
  private static DependencyGraph loadGraph(String filename) throws Exception {
    File file = new File(filename);
    if (!file.exists()) {
      System.err.println("unable to find " + filename);
      System.exit(-1);
    }

    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    DependencyGraph graph = (DependencyGraph) ois.readObject();

    return graph;
  }
  
  
  private static void printHelp() {
    System.out.println("DeadlockDetector reads serialized graphs of the state of the distributed");
    System.out.println("system created by collectDependencies.");
    System.out.println();
    System.out.println("usage: ");
    System.out.println("[print | findImpasse | findCycle | findObject objectName ] file1 ...");
    System.out.println();
    System.out.println("print - prints all dependencies and threads in the graph");
    System.out.println("findImpasse - looks for either a deadlock or the longest call chain in the graph");
    System.out.println("findCycle - looks for a deadlock");
    System.out.println("findObject - finds the given object (thread, lock, message) by name/partial name and finds all call chains leading to that object");
  }

  public static void main(String... args) throws Exception {
    if (args.length == 0) {
      printHelp();
      return;
    }
    
    DependencyGraph graph;

    switch (args[0]) {
    case "print":
      graph = loadGraphs(1, args);
      System.out.println(prettyFormat(graph));
      break;
    case "findCycle":
      graph = loadGraphs(1, args);
      List<Dependency> cycle = graph.findCycle();
      if (cycle == null) {
        System.out.println("no deadlock found");
      } else {
        System.out.println("deadlocked threads: \n" + cycle);
      }
      break;
    case "findImpasse":
      graph = loadGraphs(1, args);
      graph = graph.findLongestCallChain();
      if (graph == null) {
        System.out.println("no long call chain could be found!");
      } else {
        System.out.println("longest call chain: \n" + prettyFormat(graph));
      }
      break;
    case "findObject":
      graph = loadGraphs(2, args);
      List<DependencyGraph> graphs = graph.findDependenciesWith(args[1]);
      if (graphs.isEmpty()) {
        System.out.println("thread not found! Try using the print command to see all threads and locate the name of the one you're interested in?");
      } else {
        int numGraphs = graphs.size();
        int i=0;
        System.out.println("findObject \"" + args[1]+"\"\n\n");
        for (DependencyGraph g: graphs) {
          i += 1;
          System.out.println("graph " + i + " of " + numGraphs + ":");
          System.out.println(prettyFormat(sortDependencies(g.getEdges())));
          if (i < numGraphs) {
            System.out.println("\n\n\n");
          }
        }
      }
      break;
    default:
      printHelp();
      break;
    }
    
  }
  
}
