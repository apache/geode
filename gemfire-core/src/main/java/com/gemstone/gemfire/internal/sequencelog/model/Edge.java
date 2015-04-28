/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.model;

import java.util.SortedMap;


public class Edge {
  
  private final long timestamp;
  private final String name;
  private final String source;
  private final Vertex dest;
  private final Graph graph;
  
  public Edge(Graph graph, long timestamp, String name, String sourceName, Vertex destVertex) {
    this.graph = graph;
    this.timestamp = timestamp;
    this.name = name;
    this.source = sourceName;
    this.dest = destVertex;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dest == null) ? 0 : dest.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Edge))
      return false;
    Edge other = (Edge) obj;
    if (dest == null) {
      if (other.dest != null)
        return false;
    } else if (!dest.equals(other.dest))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }
  
  @Override
  public String toString() {
    return name;
  }

  public String getSourceName() {
    return source;
  }
  
  public String getName() {
    return name;
  }

  public Vertex getSource() {
    SortedMap<Long, Vertex> sourceMap = graph.getIndexedVertices().get(source);
    if(sourceMap == null) {
      return null;
    }
    SortedMap<Long, Vertex> headMap = sourceMap.headMap(dest.getTimestamp() + 1);
    if(headMap.isEmpty()) {
      return null;
    }
    Long closestTimestamp = headMap.lastKey();
    return headMap.get(closestTimestamp);
  }
  
  public Vertex getDest() {
    return dest;
  }

  public long getTimestamp() {
    return timestamp;
  }
}