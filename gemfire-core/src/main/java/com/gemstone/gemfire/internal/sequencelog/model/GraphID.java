/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.model;

import java.io.Serializable;

import com.gemstone.gemfire.internal.sequencelog.GraphType;

/**
 * @author dsmith
 *
 */
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof GraphID))
      return false;
    GraphID other = (GraphID) obj;
    if (graphName == null) {
      if (other.graphName != null)
        return false;
    } else if (!graphName.equals(other.graphName))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    return true;
  }

  public int compareTo(GraphID o) {
    if(o == null) {
      return -1;
    }
    int result = type.compareTo(o.getType());
    if(result != 0) {
      return result;
    }
    return graphName.compareTo(o.getGraphName());
  }
  
  
  
  
}
