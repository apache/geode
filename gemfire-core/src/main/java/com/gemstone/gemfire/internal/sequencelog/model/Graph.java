/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author dsmith
 *
 * TODO - I think a better idea here would be consider
 * source vertices as "temporary" place holders that
 * will get coalesed in the nearest destination vertex
 * in time. That might help the visualization. Or
 * maybe that should happen in that layer...
 */
public class Graph {

  private GraphID id;
  
  private Set<Edge> edges = new HashSet<Edge>();
  //A map used to find vertices by location id and timestamp.
  //locationId-> map(timestamp->vertex)
  private Map<String, SortedMap<Long, Vertex>> indexedVertices = new HashMap<String, SortedMap<Long, Vertex>>();

  public Graph(GraphID id) {
    this.id = id;
  }

  /**
   * Add an edge to this graph.
   * @param timestamp
   * @param edgeName
   * @param source
   * @param dest
   * @param isFromPattern 
   */
  public void addEdge(long timestamp, String edgeName, String state, String source,
      String dest, boolean isFromPattern) {

    Vertex destVertex = new Vertex(this, dest, state, timestamp);
    SortedMap<Long, Vertex> map  = this.indexedVertices.get(dest);
    if(map == null) {
      map = new TreeMap<Long, Vertex>();
      this.indexedVertices.put(dest, map);
    }
    
    //If this edge is being added by a pattern event, only
    //add the edge if the destination changes state as a result
    //of this edge. This cuts down on noise in the graph.
    if(isFromPattern) {
      SortedMap<Long, Vertex> headMap = map.headMap(timestamp);
      if(headMap != null && !headMap.isEmpty()) {
        Long previousKey = headMap.lastKey();
        Vertex previousVertex = headMap.get(previousKey);
        if(previousVertex.getState().equals(state)) {
          return;
        }
      } else {
        //Super hack here. Don't add a transition from the non existent state to
        //the destroyed state in a lifeline.
        if(state.equals("destroyed")) {
          return;
        }
      }
    }
    map.put(timestamp, destVertex);
    
    edges.add(new Edge(this, timestamp, edgeName, source, destVertex));
  }

  /**
   * Get the edges in the graph.
   */
  public Collection<Edge> getEdges() {
    return edges;
  }

  /**
   * Get the vertices in this graph, grouped by location id and then sorted
   * by timestamp.
   */
  Map<String, SortedMap<Long, Vertex>> getIndexedVertices() {
    return indexedVertices;
  }
}
