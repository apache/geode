/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

/**
 * A logger that allows the user to "log" events in a sequence diagram.
 * Useful for tracking the movement of an object through our 
 * distributed system, for example.
 * @author dsmith
 *
 */
public interface SequenceLogger {
  /**
   * A transition in the sequence diagram. We will actually be recording the toString
   * value of all of these fields if logging is enabled for this graph type.
   *
   * There can be many independent sequence diagrams that are logged. Each 
   * sequence diagram is identified by a graphName and a graphType.
   * 
   * Each log statement creates a transition in the diagram
   * <pre>
   * Source         Dest
   *   |             |
   *   |-(edgeName)->| (now in state "state")
   * </pre>
   * 
   * The graphName can a Pattern, at which point it will be considered a
   * transition for all graphNames that match the pattern at that time to the given state.
   * 
   * @param type the type of graph this is.
   * @param graphName
   *          The name of the graph we're recording. For example, an individual
   *          key, like "Object_123"
   * @param edgeName
   *          the name of the edge. For example ("put", or "GII")
   * @param state
   *          the final state of the destination. For example ("value_123", or "created"). 
   * @param source
   *          the source, for example ("member 1, region 1");
   * @param dest
   *          the destination, for example ("member 2, region 1");
   */         
  public void logTransition(GraphType type, Object graphName, Object edgeName, Object state,
      Object source, Object dest);
  
  public boolean isEnabled(GraphType type);
  
  public void flush() throws InterruptedException;
}
