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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Test;


public class OrderStatisticsTreeTest {
  private final OrderStatisticsTree<Integer> tree =
      new OrderStatisticsTree<>();

  private final TreeSet<Integer> set = new TreeSet<>();

  @After
  public void cleanup() {
    tree.clear();
    set.clear();
  }

  @Test
  public void testAdd() {
    assertThat(tree.isEmpty()).isEqualTo(set.isEmpty());

    for (int i = 10; i < 30; i += 2) {
      assertThat(tree.isHealthy()).isTrue();
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
      assertThat(tree.add(i)).isEqualTo(set.add(i));
      assertThat(tree.add(i)).isEqualTo(set.add(i));
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
      assertThat(tree.isHealthy()).isTrue();
    }

    assertThat(set.isEmpty()).isEqualTo(tree.isEmpty()).isFalse();
  }

  @Test
  public void testAddAll() {
    for (int i = 0; i < 10; ++i) {
      assertThat(tree.add(i)).isEqualTo(set.add(i));
    }

    Collection<Integer> coll = Arrays.asList(10, 9, 7, 11, 12);

    assertThat(tree.addAll(coll)).isEqualTo(set.addAll(coll));
    assertThat(tree.size()).isEqualTo(set.size());

    for (int i = -10; i < 20; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }
  }

  @Test
  public void testClear() {
    for (int i = 0; i < 2000; ++i) {
      set.add(i);
      tree.add(i);
    }

    assertThat(tree.size()).isEqualTo(set.size());
    set.clear();
    tree.clear();
    // noinspection ConstantConditions
    assertThat(set.size()).isEqualTo(tree.size()).isEqualTo(0);
  }

  @Test
  public void testContains() {
    for (int i = 100; i < 200; i += 3) {
      assertThat(tree.isHealthy()).isTrue();
      assertThat(tree.add(i)).isEqualTo(set.add(i));
      assertThat(tree.isHealthy()).isTrue();
    }

    assertThat(tree.size()).isEqualTo(set.size());

    for (int i = 0; i < 300; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }
  }

  @Test
  public void testContainsAll() {
    for (int i = 0; i < 50; ++i) {
      set.add(i);
      tree.add(i);
    }

    Collection<Integer> coll = new HashSet<>();

    for (int i = 10; i < 20; ++i) {
      coll.add(i);
    }

    assertThat(tree.containsAll(coll)).isEqualTo(set.containsAll(coll));
    coll.add(100);
    assertThat(tree.containsAll(coll)).isEqualTo(set.containsAll(coll));
  }

  @Test
  public void testRemove() {
    for (int i = 0; i < 200; ++i) {
      assertThat(tree.add(i)).isEqualTo(set.add(i));
    }

    for (int i = 50; i < 150; i += 2) {
      assertThat(tree.remove(i)).isEqualTo(set.remove(i));
      assertThat(tree.isHealthy()).isTrue();
    }

    for (int i = -100; i < 300; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }
  }

  @Test
  public void testRemoveLast() {
    tree.add(1);
    tree.remove(1);
    assertThat(tree.size()).isEqualTo(0);
  }

  @Test
  public void testRemoveAll() {
    for (int i = 0; i < 40; ++i) {
      set.add(i);
      tree.add(i);
    }

    Collection<Integer> coll = new HashSet<>();

    for (int i = 10; i < 20; ++i) {
      coll.add(i);
    }

    assertThat(tree.removeAll(coll)).isEqualTo(set.removeAll(coll));

    for (int i = -10; i < 50; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }

    assertThat(tree.removeAll(coll)).isEqualTo(set.removeAll(coll));

    for (int i = -10; i < 50; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }
  }

  @Test
  public void testSize() {
    assertThat(tree.size()).isEqualTo(set.size());
    for (int i = 0; i < 200; ++i) {
      assertThat(tree.add(i)).isEqualTo(set.add(i));
      assertThat(tree.size()).isEqualTo(set.size());
    }
  }

  @Test
  public void testIndexOf() {
    for (int i = 0; i < 100; ++i) {
      assertThat(tree.add(i * 2)).isTrue();
    }

    for (int i = 0; i < 100; ++i) {
      assertThat(tree.indexOf(2 * i)).isEqualTo(i);
    }

    for (int i = 100; i < 150; ++i) {
      assertThat(tree.indexOf(2 * i)).isEqualTo(-1);
    }
  }

  @Test
  public void testEmpty() {
    assertThat(tree.isEmpty()).isEqualTo(set.isEmpty());
    set.add(0);
    tree.add(0);
    assertThat(tree.isEmpty()).isEqualTo(set.isEmpty());
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testEmptyTreeGetThrowsOnNegativeIndex() {
    tree.get(-1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testEmptyTreeSelectThrowsOnTooLargeIndex() {
    tree.get(0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSelectThrowsOnNegativeIndex() {
    for (int i = 0; i < 5; ++i) {
      tree.add(i);
    }

    tree.get(-1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSelectThrowsOnTooLargeIndex() {
    for (int i = 0; i < 5; ++i) {
      tree.add(i);
    }

    tree.get(5);
  }

  @Test
  public void testGet() {
    for (int i = 0; i < 100; i += 3) {
      tree.add(i);
    }

    for (int i = 0; i < tree.size(); ++i) {
      assertThat(tree.get(i)).isEqualTo(Integer.valueOf(3 * i));
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testEmptyIterator() {
    tree.iterator().next();
  }

  @Test
  public void testIteratorThrowsOnDoubleRemove() {
    for (int i = 10; i < 20; ++i) {
      set.add(i);
      tree.add(i);
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    for (int i = 0; i < 3; ++i) {
      assertThat(iterator2.next()).isEqualTo(iterator1.next());
    }

    iterator1.remove();
    iterator2.remove();

    assertThatThrownBy(iterator1::remove).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(iterator2::remove).isInstanceOf(IllegalStateException.class);
  }


  @Test
  public void testIterator() {
    for (int i = 0; i < 5; ++i) {
      tree.add(i);
      set.add(i);
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    for (int i = 0; i < 5; ++i) {
      assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
      assertThat(iterator2.next()).isEqualTo(iterator1.next());
    }

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());

    assertThatThrownBy(iterator1::next).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(iterator2::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testRemoveBeforeNextThrowsEmpty() {

    assertThatThrownBy(() -> set.iterator().remove()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> tree.iterator().remove()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testRemoveThrowsWithoutNext() {
    for (int i = 0; i < 10; ++i) {
      tree.add(i);
      set.add(i);
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    for (int i = 0; i < 4; ++i) {
      assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
      assertThat(iterator2.next()).isEqualTo(iterator1.next());
    }

    iterator1.remove();
    iterator2.remove();

    assertThatThrownBy(iterator1::remove).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(iterator2::remove).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testRetainAll() {
    for (int i = 0; i < 100; ++i) {
      set.add(i);
      tree.add(i);
    }

    Collection<Integer> coll = Arrays.asList(26, 29, 25);

    assertThat(tree.retainAll(coll)).isEqualTo(set.retainAll(coll));
    assertThat(tree.size()).isEqualTo(set.size());

    assertThat(set.containsAll(tree)).isTrue();
    assertThat(tree.containsAll(set)).isTrue();
  }

  @Test
  public void testIteratorRemove() {
    for (int i = 10; i < 16; ++i) {
      assertThat(tree.add(i)).isEqualTo(set.add(i));
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
    assertThat(iterator2.next()).isEqualTo(iterator1.next());

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
    assertThat(iterator2.next()).isEqualTo(iterator1.next());

    iterator1.remove(); // remove 11
    iterator2.remove();

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
    assertThat(iterator2.next()).isEqualTo(iterator1.next());

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());
    assertThat(iterator2.next()).isEqualTo(iterator1.next());

    iterator1.remove(); // remove 13
    iterator2.remove();

    assertThat(tree.size()).isEqualTo(set.size());

    for (int i = 10; i < 16; ++i) {
      assertThat(tree.contains(i)).isEqualTo(set.contains(i));
    }
  }

  @Test
  public void testIteratorBruteForce() {
    for (int i = 0; i < 10_000; ++i) {
      assertThat(tree.add(i)).isEqualTo(set.add(i));
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    long seed = System.nanoTime();
    Random random = new Random(seed);

    System.out.println("testIteratorBruteForce - seed: " + seed);

    while (true) {
      if (!iterator1.hasNext()) {
        assertThat(iterator2.hasNext()).isFalse();
        break;
      }

      boolean toRemove = random.nextBoolean();

      if (toRemove) {
        try {
          iterator1.remove();
          iterator2.remove();
        } catch (IllegalStateException ex) {
          assertThatThrownBy(iterator2::remove).isInstanceOf(IllegalStateException.class);
        }
      } else {
        assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());

        if (iterator1.hasNext()) {
          assertThat(iterator2.next()).isEqualTo(iterator1.next());
        } else {
          break;
        }
      }
    }

    assertThat(tree.size()).isEqualTo(set.size());
    assertThat(tree.isHealthy()).isTrue();
    assertThat(set.containsAll(tree)).isTrue();
    assertThat(tree.containsAll(set)).isTrue();
  }

  @Test
  public void testIteratorConcurrentModification() {
    for (int i = 0; i < 100; ++i) {
      set.add(i);
      tree.add(i);
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    set.remove(10);
    tree.remove(10);

    assertThat(iterator2.hasNext()).isEqualTo(iterator1.hasNext());

    try {
      iterator1.next();
      assertThatCode(() -> iterator2.next()).doesNotThrowAnyException();
    } catch (ConcurrentModificationException ex) {
      assertThatThrownBy(iterator2::next)
          .isInstanceOf(ConcurrentModificationException.class);
    }
  }


  @Test
  public void testIteratorConcurrentRemove() {
    for (int i = 10; i < 20; ++i) {
      set.add(i);
      tree.add(i);
    }

    Iterator<Integer> iterator1 = set.iterator();
    Iterator<Integer> iterator2 = tree.iterator();

    for (int i = 0; i < 4; ++i) {
      iterator1.next();
      iterator2.next();
    }

    // None of them contains 2, should not change the modification count.
    set.remove(2);
    tree.remove(2);

    iterator1.remove();
    iterator2.remove();

    iterator1.next();
    iterator2.next();

    set.remove(12);
    tree.remove(12);

    // Both of them should throw.
    assertThatThrownBy(iterator1::remove)
        .isInstanceOf(ConcurrentModificationException.class);
    assertThatThrownBy(iterator2::remove)
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testConcurrentOrIllegalStateOnRemove() {
    for (int i = 0; i < 10; ++i) {
      set.add(i);
      tree.add(i);
    }

    Iterator<Integer> setIterator = set.iterator();
    Iterator<Integer> treeIterator = tree.iterator();

    set.add(100);
    tree.add(100);

    assertThatThrownBy(setIterator::remove).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(treeIterator::remove).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testConcurrentIterators() {
    for (int i = 0; i < 10; ++i) {
      set.add(i);
      tree.add(i);
    }

    Iterator<Integer> iterator1a = set.iterator();
    Iterator<Integer> iterator1b = set.iterator();
    Iterator<Integer> iterator2a = tree.iterator();
    Iterator<Integer> iterator2b = tree.iterator();

    for (int i = 0; i < 3; ++i) {
      iterator1a.next();
      iterator2a.next();
    }

    iterator1a.remove();
    iterator2a.remove();

    assertThat(iterator2b.hasNext()).isEqualTo(iterator1b.hasNext());

    assertThatThrownBy(iterator1b::next).isInstanceOf(ConcurrentModificationException.class);
    assertThatThrownBy(iterator2b::next).isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testToArray() {
    Random r = new Random();

    for (int i = 0; i < 50; ++i) {
      int num = r.nextInt();
      set.add(num);
      tree.add(num);
    }

    assertThat(tree.toArray()).isEqualTo(set.toArray());
  }

  @Test
  public void testToArrayGeneric() {
    for (int i = 0; i < 100; ++i) {
      set.add(i);
      tree.add(i);
    }

    Integer[] array1before = new Integer[99];
    Integer[] array2before = new Integer[99];

    Integer[] array1after = set.toArray(array1before);
    Integer[] array2after = tree.toArray(array2before);

    assertThat(array1after).isNotSameAs(array1before);
    assertThat(array2after).isNotSameAs(array2before);
    assertThat(array2after).isEqualTo(array1after);

    set.remove(1);
    tree.remove(1);

    array1after = set.toArray(array1before);
    array2after = tree.toArray(array2before);

    assertThat(array1after).isSameAs(array1before);
    assertThat(array2after).isSameAs(array2before);
    assertThat(array2after).isEqualTo(array1after);
  }
}
