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
