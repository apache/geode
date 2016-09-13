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
package org.apache.geode.distributed.internal.deadlock;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DependencyGraphJUnitTest {

  @Test
  public void testFindCycle() {
    DependencyGraph graph = new DependencyGraph();
    graph.addEdge(new Dependency("A", "B"));
    graph.addEdge(new Dependency("A", "F"));
    graph.addEdge(new Dependency("B", "C"));
    graph.addEdge(new Dependency("B", "D"));
    graph.addEdge(new Dependency("B", "E"));
    graph.addEdge(new Dependency("E", "A"));
    
    Set expected = new HashSet();
    expected.add(new Dependency("A", "B"));
    expected.add(new Dependency("B", "E"));
    expected.add(new Dependency("E", "A"));
    assertEquals(expected, new HashSet(graph.findCycle()));
  }

  @Test
  public void testSubGraph() {
    DependencyGraph graph = new DependencyGraph();
    graph.addEdge(new Dependency("A", "B"));
    graph.addEdge(new Dependency("B", "C"));
    graph.addEdge(new Dependency("C", "A"));
    graph.addEdge(new Dependency("E", "F"));
    graph.addEdge(new Dependency("F", "G"));
    
    DependencyGraph sub1 = graph.getSubGraph("B");
    Set expected = new HashSet();
    expected.add(new Dependency("A", "B"));
    expected.add(new Dependency("B", "C"));
    expected.add(new Dependency("C", "A"));
    assertEquals(expected, new HashSet(sub1.findCycle()));
    assertEquals(expected, new HashSet(sub1.getEdges()));
    
    DependencyGraph sub2 = graph.getSubGraph("E");
    assertEquals(null, sub2.findCycle());
  }

  @Test
  public void testTwoPaths() {
    DependencyGraph graph = new DependencyGraph();
    graph.addEdge(new Dependency("A", "B"));
    graph.addEdge(new Dependency("A", "C"));
    graph.addEdge(new Dependency("B", "D"));
    graph.addEdge(new Dependency("C", "D"));
    
    assertEquals(null, graph.findCycle());
  }

  @Test
  public void testEmptySet() {
    DependencyGraph graph = new DependencyGraph();
    assertEquals(null, graph.findCycle());
  }
}
