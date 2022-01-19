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
    source = sourceName;
    dest = destVertex;
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Edge)) {
      return false;
    }
    Edge other = (Edge) obj;
    if (dest == null) {
      if (other.dest != null) {
        return false;
      }
    } else if (!dest.equals(other.dest)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (source == null) {
      if (other.source != null) {
        return false;
      }
    } else if (!source.equals(other.source)) {
      return false;
    }
    return timestamp == other.timestamp;
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
    if (sourceMap == null) {
      return null;
    }
    SortedMap<Long, Vertex> headMap = sourceMap.headMap(dest.getTimestamp() + 1);
    if (headMap.isEmpty()) {
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
