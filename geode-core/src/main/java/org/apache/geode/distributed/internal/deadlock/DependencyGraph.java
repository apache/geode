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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.internal.deadlock.MessageDependencyMonitor.MessageKey;

/**
 * This class holds a graph of dependencies between objects
 *
 * It detects cycles in the graph by using the Depth First Search algorithm. Calling findCycle will
 * return the first cycle that is discovered in the graph.
 *
 *
 *
 */
public class DependencyGraph implements Serializable {
  private static final long serialVersionUID = -6794339771271587648L;

  /**
   * The vertices of the graph. The key is the vertex, the value is the set of outgoing dependencies
   * (ie the dependencies where this vertex is the depender).
   */
  private final Map<Object, Set<Dependency>> vertices = new LinkedHashMap();

  /**
   * The edges of the graph. This holds all of the dependencies in the graph.
   */
  private final Set<Dependency> edges = new LinkedHashSet<Dependency>();

  /** add a collection of edges to this graph */
  public void addEdges(Collection<Dependency> edges) {
    for (Dependency dep : edges) {
      addEdge(dep);
    }
  }

  /**
   * Add an edge to the dependency graph.
   */
  public void addEdge(Dependency dependency) {
    if (!edges.contains(dependency)) {
      edges.add(dependency);
      Set<Dependency> outboundEdges = vertices.get(dependency.getDepender());
      if (outboundEdges == null) {
        outboundEdges = new HashSet();
        vertices.put(dependency.getDepender(), outboundEdges);
      }
      outboundEdges.add(dependency);

      if (vertices.get(dependency.getDependsOn()) == null) {
        vertices.put(dependency.getDependsOn(), new HashSet());
      }
    }
  }

  /**
   * Find a cycle in the graph, if one exists.
   *
   * This method works by starting at any vertex and doing a depth first search. If it ever
   * encounters a vertex that is currently in the middle of the search (as opposed to a vertex whose
   * dependencies have been completely analyzed), then it returns the chain that starts from our
   * start vertex and includes the cycle.
   */
  public LinkedList<Dependency> findCycle() {

    Set<Object> unvisited = new HashSet<Object>(vertices.keySet());
    Set<Object> finished = new HashSet<Object>(vertices.size());

    while (unvisited.size() > 0) {
      Object start = unvisited.iterator().next();
      CycleHolder cycle = new CycleHolder();

      boolean foundCycle = visitCycle(start, unvisited, finished, cycle, 0);
      if (foundCycle) {
        return cycle.cycle;
      }
    }

    return null;
  }

  /**
   * This will find the longest call chain in the graph. If a cycle is detected it will be returned.
   * Otherwise all subgraphs are traversed to find the one that has the most depth. This usually
   * indicates the thread that is blocking the most other threads.
   * <p>
   *
   * The findDependenciesWith method can then be used to find all top-level threads that are blocked
   * by the culprit.
   */
  public DependencyGraph findLongestCallChain() {
    int depth = 0;
    DependencyGraph deepest = null;

    for (Object dep : vertices.keySet()) {
      int itsDepth = getDepth(dep);
      if (itsDepth > depth) {
        deepest = getSubGraph(dep);
        depth = itsDepth;
      }
    }

    return deepest;
  }



  /**
   * This returns a collection of top-level threads and their path to the given object. The object
   * name is some substring of the toString of the object
   *
   */
  public List<DependencyGraph> findDependenciesWith(String objectName) {

    // first find a dependency containing the node. If we can't find one
    // we can just quit
    Object obj = null;
    Dependency objDep = null;
    for (Dependency dep : edges) {
      if (dep.depender.toString().contains(objectName)) {
        obj = dep.depender;
        objDep = dep;
        break;
      }
      if (dep.dependsOn.toString().contains(objectName)) {
        obj = dep.dependsOn;
        objDep = dep;
        break;
      }
    }
    if (obj == null) {
      return Collections.emptyList();
    }

    // expand the dependency set to include all incoming
    // references, references to those references, etc.
    // This should give us a collection that includes all
    // top-level nodes that have no references to them
    Set<Object> dependsOnObj = new HashSet<>();
    dependsOnObj.add(obj);
    boolean anyAdded = true;
    while (anyAdded) {
      anyAdded = false;
      for (Dependency dep : edges) {
        if (dependsOnObj.contains(dep.dependsOn) && !dependsOnObj.contains(dep.depender)) {
          anyAdded = true;
          dependsOnObj.add(dep.depender);
        }
      }
    }

    // find all terminal nodes having no incoming
    // dependers.
    Set<Object> allDependents = new HashSet<>();
    for (Dependency dep : edges) {
      if ((dep.dependsOn instanceof LocalThread)) {
        if (dep.depender instanceof MessageKey) {
          allDependents.add(dep.dependsOn);
        }
      } else {
        allDependents.add(dep.dependsOn);
      }
    }

    List<DependencyGraph> result = new LinkedList<>();
    for (Object depender : dependsOnObj) {
      if (!allDependents.contains(depender)) {
        result.add(getSubGraph(depender));
      }
    }

    return result;
  }

  /**
   * Visit a vertex for the purposes of finding a cycle in the graph.
   *
   * @param start the node
   * @param unvisited the set of vertices that have not yet been visited
   * @param finished the set of vertices that have been completely analyzed
   * @param cycle an object used to record the any cycles that are detected
   * @param depth the depth of the recursion chain up to this point
   */
  private boolean visitCycle(Object start, Set<Object> unvisited, Set<Object> finished,
      CycleHolder cycle, int depth) {
    if (finished.contains(start)) {
      return false;
    }

    if (!unvisited.remove(start)) {
      return true;
    }

    cycle.processDepth(depth);

    boolean foundCycle = false;
    for (Dependency dep : vertices.get(start)) {
      foundCycle |= visitCycle(dep.getDependsOn(), unvisited, finished, cycle, depth + 1);
      if (foundCycle) {
        cycle.add(dep);
        break;
      }
    }
    finished.add(start);

    return foundCycle;
  }

  /** return the depth of the subgraph for the given object */
  private int getDepth(Object depender) {
    Set<Object> unvisited = new HashSet<Object>(vertices.keySet());
    Set<Object> finished = new HashSet<Object>(vertices.size());

    Object start = depender;
    CycleHolder cycle = new CycleHolder();

    boolean foundCycle = visitCycle(start, unvisited, finished, cycle, 0);

    if (foundCycle) {
      return Integer.MAX_VALUE;
    } else {
      return cycle.getMaxDepth();
    }
  }


  /**
   * Get the subgraph that the starting object has dependencies on.
   *
   * This does not include any objects that have dependencies on the starting object.
   */
  public DependencyGraph getSubGraph(Object start) {
    DependencyGraph result = new DependencyGraph();

    populateSubGraph(start, result);

    return result;
  }



  private void populateSubGraph(Object start, DependencyGraph result) {
    if (result.vertices.containsKey(start)) {
      return;
    }
    if (vertices.get(start) == null) {
      return;
    }

    result.addVertex(start, vertices.get(start));
    for (Dependency dep : result.vertices.get(start)) {
      populateSubGraph(dep.getDependsOn(), result);
    }
  }

  /**
   * Add a vertex to the graph.
   */
  private void addVertex(Object start, Set<Dependency> set) {
    vertices.put(start, set);
    edges.addAll(set);
  }

  public Collection<Dependency> getEdges() {
    return edges;
  }

  public Collection<Object> getVertices() {
    return vertices.keySet();
  }

  private static class CycleHolder {
    private final LinkedList<Dependency> cycle = new LinkedList<Dependency>();
    private boolean cycleDone;
    private int maxDepth = 0;

    public void processDepth(int depth) {
      if (depth > maxDepth) {
        maxDepth = depth;
      }
    }

    public int getMaxDepth() {
      return maxDepth;
    }

    public void add(Dependency dep) {
      if (cycleDone) {
        return;
      }

      cycle.addFirst(dep);

      Object lastVertex = cycle.getLast().getDependsOn();

      if (dep.depender.equals(lastVertex)) {
        cycleDone = true;
      }
    }
  }

}
