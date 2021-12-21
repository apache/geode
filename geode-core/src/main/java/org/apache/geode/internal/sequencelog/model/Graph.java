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
package org.apache.geode.internal.sequencelog.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * TODO - I think a better idea here would be consider source vertices as "temporary" place holders
 * that will get coalesed in the nearest destination vertex in time. That might help the
 * visualization. Or maybe that should happen in that layer...
 */
public class Graph {

  private final GraphID id;

  private final Set<Edge> edges = new HashSet<Edge>();
  // A map used to find vertices by location id and timestamp.
  // locationId-> map(timestamp->vertex)
  private final Map<String, SortedMap<Long, Vertex>> indexedVertices =
      new HashMap<String, SortedMap<Long, Vertex>>();

  public Graph(GraphID id) {
    this.id = id;
  }

  /**
   * Add an edge to this graph.
   *
   */
  public void addEdge(long timestamp, String edgeName, String state, String source, String dest,
      boolean isFromPattern) {

    Vertex destVertex = new Vertex(this, dest, state, timestamp);
    SortedMap<Long, Vertex> map = indexedVertices.get(dest);
    if (map == null) {
      map = new TreeMap<Long, Vertex>();
      indexedVertices.put(dest, map);
    }

    // If this edge is being added by a pattern event, only
    // add the edge if the destination changes state as a result
    // of this edge. This cuts down on noise in the graph.
    if (isFromPattern) {
      SortedMap<Long, Vertex> headMap = map.headMap(timestamp);
      if (headMap != null && !headMap.isEmpty()) {
        Long previousKey = headMap.lastKey();
        Vertex previousVertex = headMap.get(previousKey);
        if (previousVertex.getState().equals(state)) {
          return;
        }
      } else {
        // Super hack here. Don't add a transition from the non existent state to
        // the destroyed state in a lifeline.
        if (state.equals("destroyed")) {
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
   * Get the vertices in this graph, grouped by location id and then sorted by timestamp.
   */
  Map<String, SortedMap<Long, Vertex>> getIndexedVertices() {
    return indexedVertices;
  }
}
