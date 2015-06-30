/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.util.Iterator;

/**
 * Provides an {@link Iterator} that allows access to the current iteration
 * element.  The implementor must provide access to the current element
 * as well as a means to move to the next element.
 * 
 * @author bakera
 *
 * @param <E> the element type
 */
public interface CursorIterator<E> extends Iterator<E> {
  /**
   * Returns the element at the current position.
   * @return the current element
   */
  E current();
  
  /**
   * Provides an iteration cursor by wrapping an {@link Iterator}.
   *
   * @param <E> the element type
   */
  public static class WrappedIterator<E> implements CursorIterator<E> {
    /** the underlying iterator */
    private final Iterator<E> src;
    
    /** the current iteration element */
    private E current;
    
    public WrappedIterator(Iterator<E> src) {
      this.src = src;
    }

    @Override
    public boolean hasNext() {
      return src.hasNext();
    }

    @Override
    public E next() {
      current = src.next();
      return current;
    }

    @Override
    public E current() {
      return current;
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
    /**
     * Returns the unwrapped interator.
     * @return the iterator
     */
    public Iterator<E> unwrap() {
      return src;
    }
  }
}
