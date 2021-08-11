/*
 * MIT License
 *
 * Copyright (c) 2021 Rodion Efremov
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * This file originally came from https://github.com/coderodde/OrderStatisticTree
 */

package org.apache.geode.redis.internal.collections;

import java.util.Iterator;
import java.util.Set;

import org.apache.geode.internal.size.Sizeable;

/**
 * This interface defines the API for an order statistic set. An order statistic
 * set is a sorted set that provides two additional methods:
 * <ul>
 * <li><code>get(int index)</code> returns the <code><index</code>th smallest
 * element,</li>
 * <li><code>indexOf(T element)</code> returns the index of the input element.
 * </li>
 * </ul>
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Feb 16, 2016)
 */
public interface OrderStatisticsSet<T> extends Set<T>, Sizeable {

  /**
   * Returns the <code>index</code>th smallest element from this set.
   *
   * @param index the element index.
   * @return the <code>index</code>th smallest element.
   */
  T get(int index);

  /**
   * Returns the index of <code>element</code> in the sorted set. If the element is not present in
   * the set, returns the index it would have if it were present.
   *
   * @param element the query element.
   * @return the index of the query element.
   */
  int indexOf(T element);

  /**
   * Returns an iterator over a range of <code>elements</code> between min and max.
   *
   * @param startIndex the index of the element at which to start the iterator.
   * @param maxElements the maximum number of elements to allow the iterator to iterate over.
   * @param reverseRange if true, the elements are considered to be ordered from the highest to the
   *        lowest score
   * @return an Iterator of <code>elements</code>.
   */
  Iterator<T> getIndexRange(int startIndex, int maxElements, boolean reverseRange);
}
