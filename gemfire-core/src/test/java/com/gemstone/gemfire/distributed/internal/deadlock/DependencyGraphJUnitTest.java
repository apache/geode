/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.util.HashSet;
import java.util.Set;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class DependencyGraphJUnitTest extends TestCase {
  
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
  
  public void testTwoPaths() {
    DependencyGraph graph = new DependencyGraph();
    graph.addEdge(new Dependency("A", "B"));
    graph.addEdge(new Dependency("A", "C"));
    graph.addEdge(new Dependency("B", "D"));
    graph.addEdge(new Dependency("C", "D"));
    
    assertEquals(null, graph.findCycle());
  }
  
  public void testEmptySet() {
    DependencyGraph graph = new DependencyGraph();
    assertEquals(null, graph.findCycle());
  }
}
