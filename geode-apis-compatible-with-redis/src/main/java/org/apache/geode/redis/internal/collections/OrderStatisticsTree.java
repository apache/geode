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


import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * This class implements an order statistic tree which is based on AVL-trees.
 *
 * @param <E> the actual element type.
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Feb 11, 2016)
 */
public class OrderStatisticsTree<E extends Comparable<? super E>>
    implements OrderStatisticsSet<E> {
  private Node<E> root;
  private int size;
  private int modCount;

  @Override
  public Iterator<E> iterator() {
    return new TreeIterator();
  }

  @Override
  public Object[] toArray() {
    Object[] array = new Object[size];
    Iterator<E> iterator = iterator();
    int index = 0;

    while (iterator.hasNext()) {
      array[index++] = iterator.next();
    }

    return array;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Iterator<E> iterator = iterator();

    if (size > a.length) {
      a = Arrays.copyOf(a, size);
    }

    int index = 0;

    for (; index < size; ++index) {
      a[index] = uncheckedCast(iterator.next());
    }

    if (index < a.length) {
      a[index] = null;
    }

    return a;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object element : c) {
      if (!contains(element)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean modified = false;

    for (E element : c) {
      if (add(element)) {
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Iterator<E> iterator = iterator();
    boolean modified = false;

    while (iterator.hasNext()) {
      E element = iterator.next();

      if (!c.contains(element)) {
        iterator.remove();
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;

    for (Object element : c) {
      if (remove(element)) {
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean add(E element) {
    Objects.requireNonNull(element, "The input element is null.");

    if (root == null) {
      root = new Node<>(element);
      size = 1;
      modCount++;
      return true;
    }

    Node<E> parent = null;
    Node<E> node = root;
    int cmp;

    while (node != null) {
      cmp = element.compareTo(node.key);

      if (cmp == 0) {
        // The element is already in this tree.
        return false;
      }

      parent = node;

      if (cmp < 0) {
        node = node.left;
      } else {
        node = node.right;
      }
    }

    Node<E> newNode = new Node<>(element);

    if (element.compareTo(parent.key) < 0) {
      parent.left = newNode;
    } else {
      parent.right = newNode;
    }

    newNode.parent = parent;
    size++;
    modCount++;
    Node<E> hi = parent;
    Node<E> lo = newNode;

    while (hi != null) {
      if (hi.left == lo) {
        hi.count++;
      }

      lo = hi;
      hi = hi.parent;
    }

    fixAfterModification(newNode, true);
    return true;
  }

  public ArrayList<E> getIndexRange(int min, int max) {
    ArrayList<E> entryList = new ArrayList<>();
    Node<E> current = getNode(min);
    for (int i = min; current != null && i <= max; i++) {
      entryList.add(current.key);
      current = successorOf(current);
    }
    return entryList;
  }

  private Node<E> getNode(int index) {
    checkIndex(index);
    Node<E> node = root;

    while (true) {
      if (index > node.count) {
        index -= node.count + 1;
        node = node.right;
      } else if (index < node.count) {
        node = node.left;
      } else {
        return node;
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(Object o) {
    return findNode((E) o) != null;
  }

  private Node<E> findNode(E o) {
    Node<E> x = root;
    int cmp;

    while (x != null && (cmp = o.compareTo(x.key)) != 0) {
      if (cmp < 0) {
        x = x.left;
      } else {
        x = x.right;
      }
    }
    return x;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    Node<E> x = findNode((E) o);

    if (x == null) {
      return false;
    }

    x = deleteNode(x);
    fixAfterModification(x, false);
    size--;
    modCount++;
    return true;
  }

  @Override
  public E get(int index) {
    checkIndex(index);
    Node<E> node = root;

    while (true) {
      if (index > node.count) {
        index -= node.count + 1;
        node = node.right;
      } else if (index < node.count) {
        node = node.left;
      } else {
        return node.key;
      }
    }
  }

  @Override
  public int indexOf(E element) {
    Node<E> node = root;

    if (root == null) {
      return -1;
    }

    int rank = root.count;
    int cmp;

    while (true) {
      if ((cmp = element.compareTo(node.key)) < 0) {
        if (node.left == null) {
          return -1;
        }

        rank -= (node.count - node.left.count);
        node = node.left;
      } else if (cmp > 0) {
        if (node.right == null) {
          return -1;
        }

        rank += 1 + node.right.count;
        node = node.right;
      } else {
        break;
      }
    }

    return rank;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public void clear() {
    modCount += size;
    root = null;
    size = 0;
  }

  @Override
  public boolean equals(Object o) {
    // self check
    if (this == o) {
      return true;
    }
    // type check and cast
    if (!(o instanceof Set)) {
      return false;
    }

    Set<?> otherSet = (Set<?>) o;

    if (this.size != otherSet.size()) {
      return false;
    }
    return this.containsAll(otherSet);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (E t : this) {
      hash += t.hashCode();
    }
    return hash;
  }

  private Node<E> successorOf(Node<E> node) {
    if (node.right != null) {
      node = node.right;

      while (node.left != null) {
        node = node.left;
      }

      return node;
    }

    Node<E> parent = node.parent;

    while (parent != null && parent.right == node) {
      node = parent;
      parent = parent.parent;
    }

    return parent;
  }

  private void checkIndex(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
          "The input index is negative: " + index);
    }

    if (index >= size) {
      throw new IndexOutOfBoundsException(
          "The input index is too large: " + index +
              ", the size of this tree is " + size);
    }
  }

  private Node<E> deleteNode(Node<E> node) {
    if (node.left == null && node.right == null) {
      // 'node' has no children.
      Node<E> parent = node.parent;

      if (parent == null) {
        // 'node' is the root node of this tree.
        root = null;
        ++modCount;
        return node;
      }

      updateParentCounts(node, parent);

      if (node == parent.left) {
        parent.left = null;
      } else {
        parent.right = null;
      }

      return node;
    }

    if (node.left != null && node.right != null) {
      // 'node' has both children.
      E tmpKey = node.key;
      Node<E> successor = minimumNode(node.right);
      node.key = successor.key;
      Node<E> child = successor.right;
      Node<E> parent = successor.parent;

      if (parent.left == successor) {
        parent.left = child;
      } else {
        parent.right = child;
      }

      if (child != null) {
        child.parent = parent;
      }

      updateParentCounts(child, parent);

      successor.key = tmpKey;
      return successor;
    }

    Node<E> child;

    // 'node' has only one child.
    if (node.left != null) {
      child = node.left;
    } else {
      child = node.right;
    }

    Node<E> parent = node.parent;
    child.parent = parent;

    if (parent == null) {
      root = child;
      return node;
    }

    if (node == parent.left) {
      parent.left = child;
    } else {
      parent.right = child;
    }

    updateParentCounts(child, parent);

    return node;

  }

  private void updateParentCounts(Node<E> lo, Node<E> hi) {
    while (hi != null) {
      if (hi.left == lo) {
        hi.count--;
      }

      lo = hi;
      hi = hi.parent;
    }
  }

  private Node<E> minimumNode(Node<E> node) {
    while (node.left != null) {
      node = node.left;
    }

    return node;
  }

  private Node<E> maximumNode(Node<E> node) {
    while (node.right != null) {
      node = node.right;
    }

    return node;
  }

  private int height(Node<E> node) {
    return node == null ? -1 : node.height;
  }

  private Node<E> leftRotate(Node<E> node1) {
    Node<E> node2 = node1.right;
    node2.parent = node1.parent;
    node1.parent = node2;
    node1.right = node2.left;
    node2.left = node1;

    if (node1.right != null) {
      node1.right.parent = node1;
    }

    node1.height = Math.max(height(node1.left), height(node1.right)) + 1;
    node2.height = Math.max(height(node2.left), height(node2.right)) + 1;
    node2.count += node1.count + 1;
    return node2;
  }

  private Node<E> rightRotate(Node<E> node1) {
    Node<E> node2 = node1.left;
    node2.parent = node1.parent;
    node1.parent = node2;
    node1.left = node2.right;
    node2.right = node1;

    if (node1.left != null) {
      node1.left.parent = node1;
    }

    node1.height = Math.max(height(node1.left), height(node1.right)) + 1;
    node2.height = Math.max(height(node2.left), height(node2.right)) + 1;
    node1.count -= node2.count + 1;
    return node2;
  }

  private Node<E> rightLeftRotate(Node<E> node1) {
    Node<E> node2 = node1.right;
    node1.right = rightRotate(node2);
    return leftRotate(node1);
  }

  private Node<E> leftRightRotate(Node<E> node1) {
    Node<E> node2 = node1.left;
    node1.left = leftRotate(node2);
    return rightRotate(node1);
  }

  // Fixing an insertion: use insertionMode = true.
  // Fixing a deletion: use insertionMode = false.
  private void fixAfterModification(Node<E> node, boolean insertionMode) {
    Node<E> parent = node.parent;
    Node<E> grandParent;
    Node<E> subTree;

    while (parent != null) {
      if (height(parent.left) == height(parent.right) + 2) {
        grandParent = parent.parent;

        if (height(parent.left.left) >= height(parent.left.right)) {
          subTree = rightRotate(parent);
        } else {
          subTree = leftRightRotate(parent);
        }

        if (updateGrandparentAfterRotation(insertionMode, parent, grandParent, subTree)) {
          return;
        }
      } else if (height(parent.right) == height(parent.left) + 2) {
        grandParent = parent.parent;

        if (height(parent.right.right) >= height(parent.right.left)) {
          subTree = leftRotate(parent);
        } else {
          subTree = rightLeftRotate(parent);
        }

        if (updateGrandparentAfterRotation(insertionMode, parent, grandParent, subTree)) {
          return;
        }
      }

      parent.height = Math.max(height(parent.left),
          height(parent.right)) + 1;
      parent = parent.parent;
    }
  }

  private boolean updateGrandparentAfterRotation(boolean insertionMode, Node<E> parent,
      Node<E> grandParent,
      Node<E> subTree) {
    if (grandParent == null) {
      root = subTree;
    } else if (grandParent.left == parent) {
      grandParent.left = subTree;
    } else {
      grandParent.right = subTree;
    }

    if (grandParent != null) {
      grandParent.height =
          Math.max(height(grandParent.left),
              height(grandParent.right)) + 1;
    }

    // Whenever fixing after insertion, at most one rotation is
    // required in order to maintain the balance.
    return insertionMode;
  }

  @VisibleForTesting
  protected boolean isHealthy() {
    if (root == null) {
      return true;
    }

    return !containsCycles()
        && heightsAreCorrect()
        && isBalanced()
        && isWellIndexed();
  }

  private boolean containsCycles() {
    Set<Node<E>> visitedNodes = new HashSet<>();
    return containsCycles(root, visitedNodes);
  }

  private boolean containsCycles(Node<E> current, Set<Node<E>> visitedNodes) {
    if (current == null) {
      return false;
    }

    if (visitedNodes.contains(current)) {
      return true;
    }

    visitedNodes.add(current);

    return containsCycles(current.left, visitedNodes)
        || containsCycles(current.right, visitedNodes);
  }

  private boolean heightsAreCorrect() {
    return getHeight(root) == root.height;
  }

  private int getHeight(Node<E> node) {
    if (node == null) {
      return -1;
    }

    int leftTreeHeight = getHeight(node.left);

    if (leftTreeHeight == Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }

    int rightTreeHeight = getHeight(node.right);

    if (rightTreeHeight == Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }

    if (node.height == Math.max(leftTreeHeight, rightTreeHeight) + 1) {
      return node.height;
    }

    return Integer.MIN_VALUE;
  }

  private boolean isBalanced() {
    return isBalanced(root);
  }

  private boolean isBalanced(Node<E> node) {
    if (node == null) {
      return true;
    }

    if (!isBalanced(node.left)) {
      return false;
    }

    if (!isBalanced(node.right)) {
      return false;
    }

    int leftHeight = height(node.left);
    int rightHeight = height(node.right);

    return Math.abs(leftHeight - rightHeight) < 2;
  }

  private boolean isWellIndexed() {
    return size == count(root);
  }

  private int count(Node<E> node) {
    if (node == null) {
      return 0;
    }

    int leftTreeSize = count(node.left);

    if (leftTreeSize == Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }

    if (node.count != leftTreeSize) {
      return Integer.MIN_VALUE;
    }

    int rightTreeSize = count(node.right);

    if (rightTreeSize == Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }

    return leftTreeSize + 1 + rightTreeSize;
  }

  private static final class Node<T> {
    T key;

    Node<T> parent;
    Node<T> left;
    Node<T> right;

    int height;
    int count;

    Node(T key) {
      this.key = key;
    }
  }

  private final class TreeIterator implements Iterator<E> {
    private Node<E> previousNode;
    private Node<E> nextNode;
    private int expectedModCount = modCount;

    TreeIterator() {
      if (root == null) {
        nextNode = null;
      } else {
        nextNode = minimumNode(root);
      }
    }

    @Override
    public boolean hasNext() {
      return nextNode != null;
    }

    @Override
    public E next() {
      if (nextNode == null) {
        throw new NoSuchElementException("Iteration exceeded.");
      }

      checkConcurrentModification();
      E datum = nextNode.key;
      previousNode = nextNode;
      nextNode = successorOf(nextNode);
      return datum;
    }

    @Override
    public void remove() {
      if (previousNode == null) {
        throw new IllegalStateException(
            nextNode == null ? "Not a single call to next(); nothing to remove."
                : "Removing the same element twice.");
      }

      checkConcurrentModification();

      Node<E> x = deleteNode(previousNode);
      fixAfterModification(x, false);

      if (x == nextNode) {
        nextNode = previousNode;
      }

      expectedModCount = ++modCount;
      size--;
      previousNode = null;
    }

    private void checkConcurrentModification() {
      if (expectedModCount != modCount) {
        throw new ConcurrentModificationException(
            "The set was modified while iterating.");
      }
    }
  }
}
