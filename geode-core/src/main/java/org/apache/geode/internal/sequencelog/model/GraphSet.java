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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.geode.internal.sequencelog.GraphType;

public class GraphSet implements GraphReaderCallback {
  private final Map<GraphID, Graph> graphs = new HashMap<>();
  private long maxTime = Long.MIN_VALUE;
  private long minTime = Long.MAX_VALUE;
  private final Map<String, Long> locations = new HashMap<>();

  private final Set<EdgePattern> edgePatterns = new TreeSet<>();

  @Override
  public void addEdge(long timestamp, GraphType graphType, String graphName, String edgeName,
      String state, String source, String dest) {
    addEdge(timestamp, graphType, graphName, edgeName, state, source, dest, false);
  }

  private void addEdge(long timestamp, GraphType graphType, String graphName, String edgeName,
      String state, String source, String dest, boolean isFromPattern) {
    if (source == null) {
      source = "ERROR_NULL";
    }
    if (dest == null) {
      dest = "ERROR_NULL";
    }
    GraphID id = new GraphID(graphType, graphName);
    Graph graph = graphs.get(id);
    if (graph == null) {
      graph = new Graph(id);
      graphs.put(id, graph);
    }
    graph.addEdge(timestamp, edgeName, state, source, dest, isFromPattern);
    if (timestamp < minTime) {
      minTime = timestamp;
    }
    if (timestamp > maxTime) {
      maxTime = timestamp;
    }

    if (source != null) {
      updateLocations(source, timestamp);
    }
    if (dest != null) {
      updateLocations(dest, timestamp);
    }

  }

  private void updateLocations(String location, long timestamp) {
    Long time = locations.get(location);
    if (time == null || time > timestamp) {
      locations.put(location, timestamp);
    }

  }

  @Override
  public void addEdgePattern(long timestamp, GraphType graphType, Pattern graphNamePattern,
      String edgeName, String state, String source, String dest) {
    edgePatterns.add(
        new EdgePattern(timestamp, graphType, graphNamePattern, edgeName, state, source, dest));

  }

  /**
   * Indicate this graphset is done populating. Triggers parsing of all of the graph patterns.
   */
  public void readingDone() {
    for (EdgePattern edgePattern : edgePatterns) {
      for (GraphID graphId : graphs.keySet()) {
        if (edgePattern.graphNamePattern.matcher(graphId.getGraphName()).matches()
            && edgePattern.graphType.equals(graphId.getType())) {
          addEdge(edgePattern.timestamp, graphId.getType(), graphId.getGraphName(),
              edgePattern.edgeName, edgePattern.state, edgePattern.source, edgePattern.dest, true);
        }
      }
    }
  }

  public Map<GraphID, Graph> getMap() {
    return graphs;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public long getMinTime() {
    return minTime;
  }

  public List<String> getLocations() {
    List<String> result = new ArrayList<>(locations.keySet());
    Collections.sort(result, (o1, o2) -> {
      Long time1 = locations.get(o1);
      Long time2 = locations.get(o2);
      return time1.compareTo(time2);
    });
    return result;
  }

  private static class EdgePattern implements Comparable<EdgePattern> {
    private final long timestamp;
    private final GraphType graphType;
    private final Pattern graphNamePattern;
    private final String edgeName;
    private final String state;
    private final String source;
    private final String dest;

    public EdgePattern(long timestamp, GraphType graphType, Pattern graphNamePattern,
        String edgeName, String state, String source, String dest) {
      this.timestamp = timestamp;
      this.graphType = graphType;
      this.graphNamePattern = graphNamePattern;
      this.edgeName = edgeName;
      this.state = state;
      this.source = source;
      this.dest = dest;
    }

    @Override
    public int compareTo(EdgePattern o) {
      int timeDifference = Long.signum(timestamp - o.timestamp);
      if (timeDifference != 0) {
        return timeDifference;
      } else {
        // don't really care about the order, but want to make them unique in the set.
        return System.identityHashCode(this) - System.identityHashCode(o);
      }
    }
  }
}
