/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: BoundedPriorityQueue.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  16Jun1998  dl               Create public version
  25aug1998  dl               added peek
  29aug1998  dl               pulled heap mechanics into separate class
*/

package com.gemstone.org.jgroups.oswego.concurrent;
import java.util.Comparator;
import java.lang.reflect.*;

/**
 * A heap-based priority queue, using semaphores for
 * concurrency control. 
 * The take operation returns the <em>least</em> element
 * with respect to the given ordering. (If more than
 * one element is tied for least value, one of them is
 * arbitrarily chosen to be returned -- no guarantees
 * are made for ordering across ties.)
 * Ordering follows the JDK1.2 collection
 * conventions: Either the elements must be Comparable, or
 * a Comparator must be supplied. Comparison failures throw
 * ClassCastExceptions during insertions and extractions.
 * The implementation uses a standard array-based heap algorithm,
 * as described in just about any data structures textbook.
 * <p>
 * Put and take operations may throw ClassCastException 
 * if elements are not Comparable, or
 * not comparable using the supplied comparator. 
 * Since not all elements are compared on each operation
 * it is possible that an exception will not be thrown 
 * during insertion of non-comparable element, but will later be 
 * encountered during another insertion or extraction.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class BoundedPriorityQueue extends SemaphoreControlledChannel {
  protected final Heap heap_;

  /**
   * Create a priority queue with the given capacity and comparator
   * @exception IllegalArgumentException if capacity less or equal to zero
   **/

  public BoundedPriorityQueue(int capacity, Comparator cmp) 
   throws IllegalArgumentException {
    super(capacity);
    heap_ = new Heap(capacity, cmp);
  }

  /**
   * Create a priority queue with the current default capacity
   * and the given comparator
   **/

  public BoundedPriorityQueue(Comparator comparator) { 
    this(DefaultChannelCapacity.get(), comparator); 
  }

  /**
   * Create a priority queue with the given capacity,
   * and relying on natural ordering.
   **/

  public BoundedPriorityQueue(int capacity) { 
    this(capacity, null); 
  }

  /**
   * Create a priority queue with the current default capacity
   * and relying on natural ordering.
   **/

  public BoundedPriorityQueue() { 
    this(DefaultChannelCapacity.get(), null); 
  }


  /**
   * Create a priority queue with the given capacity and comparator, using
   * the supplied Semaphore class for semaphores.
   * @exception IllegalArgumentException if capacity less or equal to zero
   * @exception NoSuchMethodException If class does not have constructor 
   * that intializes permits
   * @exception SecurityException if constructor information 
   * not accessible
   * @exception InstantiationException if semaphore class is abstract
   * @exception IllegalAccessException if constructor cannot be called
   * @exception InvocationTargetException if semaphore constructor throws an
   * exception
   **/

  public BoundedPriorityQueue(int capacity, Comparator cmp, 
                              Class semaphoreClass) 
   throws IllegalArgumentException, 
          NoSuchMethodException, 
          SecurityException, 
          InstantiationException, 
          IllegalAccessException, 
          InvocationTargetException {
    super(capacity, semaphoreClass);
    heap_ = new Heap(capacity, cmp);
  }

  @Override // GemStoneAddition
  protected void insert(Object x) {  heap_.insert(x);  }
  @Override // GemStoneAddition
  protected Object extract()      { return heap_.extract(); }
  public Object peek()            { return heap_.peek(); }

}
