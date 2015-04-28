package com.gemstone.gemfire.internal.util.concurrent;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.concurrent.CompactConcurrentHashSet2;
import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class CompactConcurrentHashSetJUnitTest {
  static int RANGE = 100000;
  static int SET_SIZE = 1000;
  Random ran = new Random();
  
  @Test
  public void testEquals() {
    Set s1, s2;
    s1 = new CompactConcurrentHashSet2();
    s2 = new HashSet();
    for (int i=0; i<10000; i++) {
      int nexti = ran.nextInt(RANGE);
      s1.add(nexti);
      s2.add(nexti);
      assertTrue("expected s1 and s2 to be equal", s1.equals(s2));
      assertTrue("expected s1 and s2 to be equal", s2.equals(s1));
    }
    assertTrue(s1.hashCode() != 0);
    s2 = new CompactConcurrentHashSet2(2);
    s2.addAll(s1);
    assertTrue("expected s1 and s2 to be equal", s1.equals(s2));
    assertTrue("expected s1 and s2 to be equal", s2.equals(s1));
  }

  @Test
  public void testIterator() {
    Set<Integer> s1;
    s1 = new CompactConcurrentHashSet2<Integer>();
    for (int i=0; i<10000; i++) {
      int nexti = ran.nextInt(RANGE);
      s1.add(nexti);
    }
    for (Iterator<Integer> it=s1.iterator(); it.hasNext(); ) {
      Integer i = it.next();
      assertTrue(s1.contains(i));
      it.remove();
      if (s1.contains(i)) {
        assertTrue(!s1.contains(i));
      }
    }
    assertTrue(s1.size() == 0);
  }

  @Test
  public void testSize() {
    Set<Integer> s1, s2;
    s1 = new CompactConcurrentHashSet2<Integer>();
    s2 = new HashSet<Integer>();
    for (int i=0; i<10000; i++) {
      int nexti = ran.nextInt(RANGE);
      s1.add(nexti);
      s2.add(nexti);
    }
    int size = s2.size(); // trust HashSet.size()
    assertTrue(s1.size() == size);
    s2 = new CompactConcurrentHashSet2<Integer>(s1);
    assertTrue(s2.size() == size);
    int i = size-1;
    for (Iterator<Integer> it = s2.iterator(); it.hasNext(); i--) {
      s1.remove(it.next());
      assertTrue(s1.size() == i);
    }
    assertTrue(s1.size() == 0);
    assertTrue(s1.isEmpty());
    assertTrue( !s2.isEmpty() );
    s2.clear();
    assertTrue(s2.isEmpty());
  }

}
