package com.gemstone.gemfire.cache.query.internal.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Utility Iterator which provides limit functionality on a given iterator
 * @author asif
 *
 * @param <E>
 */
public class LimitIterator<E> implements Iterator<E> {

  private final Iterator<E> rootIter;
  private final int limit;
  private int numIterated = 0;

  public LimitIterator(Iterator<E> rootIter, int limit) {
    this.rootIter = rootIter;
    this.limit = limit;
  }

  @Override
  public boolean hasNext() {
    if (this.numIterated < this.limit) {
      return this.rootIter.hasNext();
    } else {
      return false;
    }
  }

  @Override
  public E next() {
    if (this.numIterated < this.limit) {
      ++this.numIterated;
      return this.rootIter.next();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Removal from limit based collection not supported");

  }
}
