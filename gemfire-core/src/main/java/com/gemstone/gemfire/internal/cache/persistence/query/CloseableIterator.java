package com.gemstone.gemfire.internal.cache.persistence.query;

import java.util.Iterator;

/**
 * An iterator that has an additional close method.
 * The iterator should be closed if it is abandoned before
 * reaching the end of the iteration to free up resources.
 * @author dsmith
 *
 */
public interface CloseableIterator<E> extends Iterator<E> {
  /**
   * Free up resources associated with this iterator.
   */
  void close();
}