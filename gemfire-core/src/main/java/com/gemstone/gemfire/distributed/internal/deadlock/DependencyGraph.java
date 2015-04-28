/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * This class holds a graph of dependencies between objects
 * 
 * It detects cycles in the graph by using the Depth First Search algorithm.
 * Calling findCycle will return the first cycle that is discovered in the
 * graph.
 * 
 * 
 * @author dsmith
 * 
 */
public class DependencyGraph implements Serializable {
  private static final long serialVersionUID = -6794339771271587648L;

  /**
   * The vertices of the graph. The key is the vertex, the value is the set of
   * outgoing dependencies (ie the dependencies where this vertex is the
   * depender).
   */
  private Map<Object, Set<Dependency>> vertices = new LinkedHashMap();

  /**
   * The edges of the graph. This holds all of the dependencies in the graph.
   */
  private Set<Dependency> edges = new LinkedHashSet<Dependency>();
 
  /** 
   * Add an edge to the dependency graph. 
   */
  public void addEdge(Dependency dependency) {
    edges.add(dependency);
    Set<Dependency> outboundEdges = vertices.get(dependency.getDepender());
    if(outboundEdges == null) {
      outboundEdges = new HashSet();
      vertices.put(dependency.getDepender(), outboundEdges);
    }
    outboundEdges.add(dependency);
    
    if(vertices.get(dependency.getDependsOn()) == null) {
      vertices.put(dependency.getDependsOn(), new HashSet());
    }
    
  }
  
  /**
   * Find a cycle in the graph, if one exists.
   * 
   * This method works by starting at any vertex and doing a depth first search.
   * If it ever encounters a vertex that is currently in the middle of the search
   * (as opposed to a vertex whose dependencies have been completely analyzed), then
   * it returns the chain that starts from our start vertex and includes the cycle.
   */
  public LinkedList<Dependency> findCycle() {

    Set<Object> unvisited = new HashSet<Object>(vertices.keySet());
    Set<Object> finished = new HashSet<Object>(vertices.size());
    
    while(unvisited.size() > 0) {
      Object start = unvisited.iterator().next();
      CycleHolder cycle = new CycleHolder();
      
      boolean foundCycle = visitCycle(start, unvisited, finished, cycle);
      if(foundCycle) {
        return cycle.cycle;
      }
    }
    
    return null;
  }

  /**
   * Visit a vertex for the purposes of finding a cycle in the graph.
   * 
   * @param start
   *          the node
   * @param unvisited
   *          the set of vertices that have not yet been visited
   * @param finished
   *          the set of vertices that have been completely analyzed
   * @param cycle
   *          an object used to record the any cycles that are detected
   */
  private boolean visitCycle(Object start, Set<Object> unvisited,
      Set<Object> finished, CycleHolder cycle) {
    if(finished.contains(start)) {
      return false;
    }
    
    if(!unvisited.remove(start)) {
      return true;
    }
    
    boolean foundCycle = false;
    for(Dependency dep : vertices.get(start)) {
      foundCycle |= visitCycle(dep.getDependsOn(), unvisited, finished, cycle);
      if(foundCycle) {
        cycle.add(dep);
        break;
      }
    }
    finished.add(start);
    
    return foundCycle;
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
    if(result.vertices.keySet().contains(start)) {
      return;
    }
    if(vertices.get(start) == null) {
      return;
    }
    
    result.addVertex(start, vertices.get(start));
    for(Dependency dep: result.vertices.get(start)) {
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
    private LinkedList<Dependency> cycle = new LinkedList<Dependency>();
    private boolean cycleDone;
    
    public void add(Dependency dep) {
      if(cycleDone) {
        return;
      }
      
      cycle.addFirst(dep);
      
      Object lastVertex = cycle.getLast().getDependsOn();
      
      if(dep.depender.equals(lastVertex)) {
        cycleDone = true;
      }
    }
  }
}
