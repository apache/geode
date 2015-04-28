/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

/**
 * @author dsmith
 *
 */
public class Transition {

  private final long timestamp;
  private final GraphType type;
  private final Object graphName;
  private final Object edgeName;
  private final Object source;
  private final Object dest;
  private final Object state;
  

  public Transition(GraphType type, Object graphName, Object edgeName, Object state, Object source,
      Object dest) {
    this.timestamp = System.currentTimeMillis();
    this.type = type;
    this.graphName = graphName;
    this.edgeName = edgeName;
    this.state = state;
    this.source = source;
    this.dest = dest;
  }
  
  public Transition(long timestamp, GraphType type, Object graphName, Object edgeName, Object state, Object source,
      Object dest) {
    this.timestamp = timestamp;
    this.type = type;
    this.graphName = graphName;
    this.edgeName = edgeName;
    this.state = state;
    this.source = source;
    this.dest = dest;
  }


  public long getTimestamp() {
    return timestamp;
  }


  public GraphType getType() {
    return type;
  }


  public Object getGraphName() {
    return graphName;
  }


  public Object getEdgeName() {
    return edgeName;
  }
  
  public Object getState() {
    return state;
  }


  public Object getSource() {
    return source;
  }

  public Object getDest() {
    return dest;
  }
}
