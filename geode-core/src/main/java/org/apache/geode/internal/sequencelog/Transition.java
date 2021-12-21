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
package org.apache.geode.internal.sequencelog;

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
    timestamp = System.currentTimeMillis();
    this.type = type;
    this.graphName = graphName;
    this.edgeName = edgeName;
    this.state = state;
    this.source = source;
    this.dest = dest;
  }

  public Transition(long timestamp, GraphType type, Object graphName, Object edgeName, Object state,
      Object source, Object dest) {
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
