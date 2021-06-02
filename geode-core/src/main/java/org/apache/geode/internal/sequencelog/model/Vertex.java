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

public class Vertex implements Comparable<Vertex> {

  private final Graph graph;
  private final String name;
  private final long timestamp;
  private final String state;


  public Vertex(Graph graph, String source, String state, long timestamp) {
    this.graph = graph;
    this.name = source;
    this.state = state;
    this.timestamp = timestamp;
  }


  public String getName() {
    return name;
  }


  public long getTimestamp() {
    return timestamp;
  }

  public String getState() {
    return state;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((state == null) ? 0 : state.hashCode());
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
    if (!(obj instanceof Vertex)) {
      return false;
    }
    Vertex other = (Vertex) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (state == null) {
      if (other.state != null) {
        return false;
      }
    } else if (!state.equals(other.state)) {
      return false;
    }
    if (timestamp != other.timestamp) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Vertex o) {
    int difference = o.name == null ? (this.name == null ? 0 : -1) : (this.name == null ? 1 : 0);
    if (difference != 0) {
      return difference;
    }
    difference = o.name.compareTo(this.name);
    if (difference != 0) {
      return difference;
    }
    difference = o.timestamp > this.timestamp ? 1 : (o.timestamp == this.timestamp ? 0 : -1);
    if (difference != 0) {
      return difference;
    }
    difference = o.state == null ? (this.state == null ? 0 : -1)
        : (this.state == null ? 1 : o.state.compareTo(this.state));

    return difference;
  }


  public Vertex getNextVertexOnDest() {
    SortedMap<Long, Vertex> map = graph.getIndexedVertices().get(name);
    SortedMap<Long, Vertex> tailMap = map.tailMap(timestamp + 1);
    if (tailMap.isEmpty()) {
      return null;
    } else {
      return tailMap.get(tailMap.firstKey());
    }
  }
}
