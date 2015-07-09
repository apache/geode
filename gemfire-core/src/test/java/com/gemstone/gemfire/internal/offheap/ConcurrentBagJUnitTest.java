package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ConcurrentBag.Node;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConcurrentBagJUnitTest {
  
  public static class IntNode implements SimpleMemoryAllocatorImpl.ConcurrentBag.Node {

    private final int data;
    
    public IntNode(int i) {
      this.data = i;
    }
    
    public Integer getData() {
      return this.data;
    }
    
    private Node next;
    @Override
    public void setNextCBNode(Node next) {
      this.next = next;
    }
    @Override
    public Node getNextCBNode() {
      return this.next;
    }
    
  }
  @Test
  public void testBasicFreeList() {
    SimpleMemoryAllocatorImpl.ConcurrentBag<IntNode> l = new SimpleMemoryAllocatorImpl.ConcurrentBag<IntNode>(5000);
    assertEquals(false, l.iterator().hasNext());
    try {
      l.iterator().next();
      fail("expected NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }
    assertEquals(null, l.poll());
    
    l.offer(new IntNode(1));
    assertEquals(true, l.iterator().hasNext());
    
    assertEquals(Integer.valueOf(1), l.iterator().next().getData());
    assertEquals(Integer.valueOf(1), l.poll().getData());
    assertEquals(false, l.iterator().hasNext());
    assertEquals(null, l.poll());
    
    try {
      l.iterator().remove();
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
    }
//    {
//      l.offer(new IntNode(1));
//      l.offer(new IntNode(2));
//      Iterator<IntNode> it = l.iterator();
//      assertEquals(true, it.hasNext());
//      assertEquals(Integer.valueOf(1), it.next().getData());
//      assertEquals(true, it.hasNext());
//      assertEquals(Integer.valueOf(2), it.next().getData());
//      assertEquals(false, it.hasNext());
//      
//      it = l.iterator();
//      try {
//        it.remove();
//        fail("expected IllegalStateException");
//      } catch (IllegalStateException expected) {
//      }
//      it.next();
//      it.remove();
//      try {
//        it.remove();
//        fail("expected IllegalStateException");
//      } catch (IllegalStateException expected) {
//      }
//      assertEquals(Integer.valueOf(2), it.next());
//      assertEquals(false, it.hasNext());
//      
//      assertEquals(Integer.valueOf(2), l.poll());
//      assertEquals(null, l.poll());
//    }
    
    for (int i=1; i <= 3999; i++) {
      l.offer(new IntNode(i));
    }
    {
      Iterator<IntNode> it = l.iterator();
//    for (int i=1; i <= 3999; i++) {
      for (int i=3999; i >= 1; i--) {
        assertEquals(true, it.hasNext());
        assertEquals(Integer.valueOf(i), it.next().getData());
      }
      assertEquals(false, it.hasNext());
    }
//  for (int i=1; i <= 3999; i++) {
    for (int i=3999; i >= 1; i--) {
      assertEquals(Integer.valueOf(i), l.poll().getData());
    }
    assertEquals(null, l.poll());
  }

}
