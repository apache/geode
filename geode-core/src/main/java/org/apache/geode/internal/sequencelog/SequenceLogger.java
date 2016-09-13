/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.sequencelog;

/**
 * A logger that allows the user to "log" events in a sequence diagram.
 * Useful for tracking the movement of an object through our 
 * distributed system, for example.
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
