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

import java.io.Serializable;

import org.apache.geode.internal.sequencelog.GraphType;

public class GraphID implements Comparable<GraphID>, Serializable {

  public final GraphType type;
  public final String graphName;


  public GraphID(GraphType type, String graphName) {
    this.type = type;
    this.graphName = graphName;
  }

  public GraphType getType() {
    return type;
  }

  public String getGraphName() {
    return graphName;
  }

  @Override
  public String toString() {
    return graphName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((graphName == null) ? 0 : graphName.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
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
    if (!(obj instanceof GraphID)) {
      return false;
    }
    GraphID other = (GraphID) obj;
    if (graphName == null) {
      if (other.graphName != null) {
        return false;
      }
    } else if (!graphName.equals(other.graphName)) {
      return false;
    }
    if (type == null) {
      return other.type == null;
    } else
      return type.equals(other.type);
  }

  @Override
  public int compareTo(GraphID o) {
    if (o == null) {
      return -1;
    }
    int result = type.compareTo(o.getType());
    if (result != 0) {
      return result;
    }
    return graphName.compareTo(o.getGraphName());
  }



}
