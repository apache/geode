/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class CopyOnWriteHashSetJUnitTest extends TestCase {
  
  public void testSnapshot() {
    CopyOnWriteHashSet<String> set = new CopyOnWriteHashSet<String>();
    set.add("a");
    Set<String> snap = set.getSnapshot();
    Set<String> copy = new HashSet<String>(set);
    set.add("b");
    
    assertEquals(copy, snap);
  }
  
  public void testAllMethods() throws Exception {
    
    CopyOnWriteHashSet<String> set = new CopyOnWriteHashSet<String>();
    assertTrue(set.add("a"));
    assertFalse(set.add("a"));
    Iterator itr = set.iterator();
    assertTrue(itr.hasNext());
    assertEquals("a", itr.next());
    assertFalse(itr.hasNext());
    assertEquals(1, set.size());
    
    assertTrue(set.addAll(Arrays.asList(new String[] {"b", "c", "d"})));
    assertTrue(set.contains("b"));
    assertTrue(set.contains("c"));
    assertTrue(set.contains("d"));
    
    assertTrue(set.retainAll(Arrays.asList(new String[] {"a", "b", "c"})));
    assertFalse(set.retainAll(Arrays.asList(new String[] {"a", "b", "c"})));
    
    HashSet<String> test = new HashSet<String>();
    test.addAll(Arrays.asList(new String[] {"a", "b", "c"}));
    assertEquals(test, set);
    assertEquals(set, test);
    assertEquals(test.toString(), set.toString());
    assertEquals(Arrays.asList(test.toArray()), Arrays.asList(set.toArray()));
    assertEquals(Arrays.asList(test.toArray(new String[0])), Arrays.asList(set.toArray(new String[0])));
    
    assertTrue(set.containsAll(test));
    assertTrue(set.containsAll(test));
    
    set.remove("b");
    
    assertFalse(set.containsAll(test));
    
    set.clear();
    
    set.addAll(Arrays.asList(new String[] {"b", "c", "d"}));
    
    assertTrue(set.removeAll(Arrays.asList(new String[] {"b", "c"})));
    assertFalse(set.removeAll(Arrays.asList(new String[] {"b", "c"})));
    
    assertEquals(new HashSet(Arrays.asList(new String[] {"d"})), set);
    
    ByteArrayOutputStream boas = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(boas);
    out.writeObject(set);
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(boas.toByteArray()));
    Set<String> result = (Set<String>) in.readObject();
    assertEquals(set, result);
    assertTrue(result instanceof CopyOnWriteHashSet);
  }

}
