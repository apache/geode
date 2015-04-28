/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A wrapper around an object array for storing values in index data structure
 * with minimal set of operations supported and the maximum size of 128 elements  
 * 
 * @author Tejas Nomulwar
 * @since 7.0
 */
public class IndexElemArray implements Iterable, Collection {

  private Object[] elementData;
  private volatile byte size;

  public IndexElemArray(int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
    }
    this.elementData = new Object[initialCapacity];
  }

  /**
   * Constructs an empty list with an initial capacity of ten.
   */
  public IndexElemArray() {
    this(IndexManager.INDEX_ELEMARRAY_SIZE);
  }

  /**
   * Increases the capacity of this <tt>ArrayList</tt> instance, if necessary,
   * to ensure that it can hold at least the number of elements specified by the
   * minimum capacity argument.
   * 
   * @param minCapacity
   *          the desired minimum capacity
   */
  private void ensureCapacity(int minCapacity) {
    int oldCapacity = elementData.length;
    if (minCapacity > oldCapacity) {
      int newCapacity = oldCapacity + 5;
      if (newCapacity < minCapacity) {
        newCapacity = minCapacity;
      }
      // minCapacity is usually close to size, so this is a win:
      Object[] newElementData = new Object[newCapacity];
      System.arraycopy(this.elementData, 0, newElementData, 0,
          this.elementData.length);
      elementData = newElementData;
    }
  }

  /**
   * Returns the number of elements in this list. (Warning: May not return
   * correct size always, as remove operation is not atomic)
   * 
   * @return the number of elements in this list
   */
  public int size() {
    return size;
  }

  /**
   * Returns <tt>true</tt> if this list contains no elements.
   * 
   * @return <tt>true</tt> if this list contains no elements
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns <tt>true</tt> if this list contains the specified element. More
   * formally, returns <tt>true</tt> if and only if this list contains at least
   * one element <tt>e</tt> such that
   * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
   * 
   * @param o
   *          element whose presence in this list is to be tested
   * @return <tt>true</tt> if this list contains the specified element
   */
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  /**
   * Returns the index of the first occurrence of the specified element in this
   * list, or -1 if this list does not contain the element. More formally,
   * returns the lowest index <tt>i</tt> such that
   * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
   * or -1 if there is no such index.
   */
  public int indexOf(Object o) {
    if (o == null) {
      for (int i = 0; i < size; i++)
        if (elementData[i] == null)
          return i;
    } else {
      for (int i = 0; i < size; i++)
        if (o.equals(elementData[i]))
          return i;
    }
    return -1;
  }

  /**
   * Returns the element at the specified position in this list.
   * 
   * @param index
   *          index of the element to return
   * @return the element at the specified position in this list
   * @throws IndexOutOfBoundsException
   *          
   */
  public Object get(int index) {
    RangeCheck(index);
    return elementData[index];
  }

  /**
   * Replaces the element at the specified position in this list with the
   * specified element.
   * 
   * @param index
   *          index of the element to replace
   * @param element
   *          element to be stored at the specified position
   * @return the element previously at the specified position
   * @throws IndexOutOfBoundsException
   *           
   */
  public Object set(int index, Object element) {
    RangeCheck(index);

    Object oldValue = (Object) elementData[index];
    elementData[index] = element;
    return oldValue;
  }

  /**
   * Appends the specified element to the end of this array.
   * If the array is full, creates a new array with 
   * new capacity = old capacity + 5
   * 
   * @param e
   *          element to be appended to this list
   * @return <tt>true</tt> (as specified by {@link Collection#add})
   * @throws ArrayIndexOutOfBoundsException
   */
  public synchronized boolean add(Object e) {
    ensureCapacity(size + 1);
    elementData[size] = e;
    ++size;
    return true;
  }

  /**
   * Removes the first occurrence of the specified element from this list, if it
   * is present. If the list does not contain the element, it is unchanged. More
   * formally, removes the element with the lowest index <tt>i</tt> such that
   * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
   * (if such an element exists). Returns <tt>true</tt> if this list contained
   * the specified element (or equivalently, if this list changed as a result of
   * the call).
   * 
   * @param o
   *          element to be removed from this list, if present
   * @return <tt>true</tt> if this list contained the specified element
   */
  public synchronized boolean remove(Object o) {
    if (o == null) {
      for (int index = 0; index < size; index++)
        if (elementData[index] == null) {
          fastRemove(index);
          return true;
        }
    } else {
      for (int index = 0; index < size; index++)
        if (o.equals(elementData[index])) {
          fastRemove(index);
          return true;
        }
    }
    return false;
  }

  /*
   * Private remove method that skips bounds checking and does not return the
   * value removed.
   */
  private void fastRemove(int index) {
    int len = elementData.length;
    Object[] newArray = new Object[len - 1];
    System.arraycopy(elementData, 0, newArray, 0, index);
    int numMoved = len - index - 1;
    if (numMoved > 0)
      System.arraycopy(elementData, index + 1, newArray, index, numMoved);
    elementData = newArray;
    --size;
  }

  /**
   * Removes all of the elements from this list. The list will be empty after
   * this call returns.
   */
  public void clear() {
    // Let gc do its work
    for (int i = 0; i < size; i++) {
      elementData[i] = null;
    }
    size = 0;
  }

  /**
   * Checks if the given index is in range. If not, throws an appropriate
   * runtime exception. This method does *not* check if the index is negative:
   * It is always used immediately prior to an array access, which throws an
   * ArrayIndexOutOfBoundsException if index is negative.
   */
  private void RangeCheck(int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }
  }

  @Override
  public synchronized boolean addAll(Collection c) {
    Object[] a = c.toArray();
    int numNew = a.length;
    ensureCapacity(size + numNew);
    System.arraycopy(a, 0, elementData, size, numNew);
    size += numNew;
    return numNew != 0;
  }

  @Override
  public Object[] toArray() {
    return Arrays.copyOf(elementData, size);
  }

  @Override
  public Iterator iterator() {
    return new IndexArrayListIterator();
  }

  private class IndexArrayListIterator implements Iterator {
    private byte current;
    private Object currentEntry;
    
    /**
     * Checks if the array has next element, stores reference to the current
     * element and increments cursor. This is required since an element may be
     * removed between hasNext() and next() method calls
     * 
     */
    @Override
    public boolean hasNext() {
      return current < size;
    }

    /**
     * Returns next element. But does not increment the cursor.
     * Always use hasNext() before this method call
     */
    @Override
    public Object next() {
      try {
        currentEntry = elementData[current++];
      } catch (IndexOutOfBoundsException e) {
        // Following exception must never be thrown.
        //throw new NoSuchElementException();
        return null;
      }
      return currentEntry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "remove() method is not supported");
    }

  }

  @Override
  public Object[] toArray(Object[] a) {
    throw new UnsupportedOperationException(
        "toArray(Object[] a) method is not supported");
  }

  @Override
  public boolean containsAll(Collection c) {
    throw new UnsupportedOperationException(
        "containsAll() method is not supported");
  }

  @Override
  public boolean removeAll(Collection c) {
    throw new UnsupportedOperationException(
        "removeAll() method is not supported");
  }

  @Override
  public boolean retainAll(Collection c) {
    throw new UnsupportedOperationException(
        "retainAll() method is not supported");
  }

  //for internal testing only
  public Object[] getElementData() {
    return elementData;
  }
}
