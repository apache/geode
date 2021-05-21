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

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * This class implements an order statistic tree which is based on AVL-trees.
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Feb 11, 2016)
 * @param <T> the actual element type.
 */
@SuppressWarnings("all")
public class OrderStatisticsTree<T extends Comparable<? super T>>
    implements OrderStatisticsSet<T> {

  @Override
  public Iterator<T> iterator() {
    return new TreeIterator();
  }

  private final class TreeIterator implements Iterator<T> {

    private Node<T> previousNode;
    private Node<T> nextNode;
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
    public T next() {
      if (nextNode == null) {
        throw new NoSuchElementException("Iteration exceeded.");
      }

      checkConcurrentModification();
      T datum = nextNode.key;
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

      Node<T> x = deleteNode(previousNode);
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

  @Override
  public Object[] toArray() {
    Object[] array = new Object[size];
    Iterator<T> iterator = iterator();
    int index = 0;

    while (iterator.hasNext()) {
      array[index++] = iterator.next();
    }

    return array;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    Iterator iterator = iterator();

    if (size > a.length) {
      a = Arrays.copyOf(a, size);
    }

    int index = 0;

    for (; index < size; ++index) {
      a[index] = (T) iterator.next();
    }

    if (index < a.length) {
      a[index] = null;
    }

    return a;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean containsAll(Collection<?> c) {
    for (Object element : c) {
      if (!contains((T) element)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean modified = false;

    for (T element : c) {
      if (add(element)) {
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    if (!c.getClass().equals(HashSet.class.getClass())) {
      c = new HashSet<>(c);
    }

    Iterator<T> iterator = iterator();
    boolean modified = false;

    while (iterator.hasNext()) {
      T element = iterator.next();

      if (!c.contains(element)) {
        iterator.remove();
        modified = true;
      }
    }

    return modified;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;

    for (Object element : c) {
      if (remove((T) element)) {
        modified = true;
      }
    }

    return modified;
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

  private Node<T> root;
  private int size;
  private int modCount;

  @Override
  public boolean add(T element) {
    Objects.requireNonNull(element, "The input element is null.");

    if (root == null) {
      root = new Node<>(element);
      size = 1;
      modCount++;
      return true;
    }

    Node<T> parent = null;
    Node<T> node = root;
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

    Node<T> newnode = new Node<>(element);

    if (element.compareTo(parent.key) < 0) {
      parent.left = newnode;
    } else {
      parent.right = newnode;
    }

    newnode.parent = parent;
    size++;
    modCount++;
    Node<T> hi = parent;
    Node<T> lo = newnode;

    while (hi != null) {
      if (hi.left == lo) {
        hi.count++;
      }

      lo = hi;
      hi = hi.parent;
    }

    fixAfterModification(newnode, true);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(Object o) {
    T element = (T) o;
    Node<T> x = root;
    int cmp;

    while (x != null && (cmp = element.compareTo(x.key)) != 0) {
      if (cmp < 0) {
        x = x.left;
      } else {
        x = x.right;
      }
    }

    return x != null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    T element = (T) o;
    Node<T> x = root;
    int cmp;

    while (x != null && (cmp = element.compareTo(x.key)) != 0) {
      if (cmp < 0) {
        x = x.left;
      } else {
        x = x.right;
      }
    }

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
  public T get(int index) {
    checkIndex(index);
    Node<T> node = root;

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
  public int indexOf(T element) {
    Node<T> node = root;

    if (root == null) {
      return -1;
    }

    int rank = root.count;
    int cmp;

    while (node != null) {
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

    return node == null ? -1 : rank;
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

  private Node<T> successorOf(Node<T> node) {
    if (node.right != null) {
      node = node.right;

      while (node.left != null) {
        node = node.left;
      }

      return node;
    }

    Node<T> parent = node.parent;

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

  private Node<T> deleteNode(Node<T> node) {
    if (node.left == null && node.right == null) {
      // 'node' has no children.
      Node<T> parent = node.parent;

      if (parent == null) {
        // 'node' is the root node of this tree.
        root = null;
        ++modCount;
        return node;
      }

      Node<T> lo = node;
      Node<T> hi = parent;

      while (hi != null) {
        if (hi.left == lo) {
          hi.count--;
        }

        lo = hi;
        hi = hi.parent;
      }

      if (node == parent.left) {
        parent.left = null;
      } else {
        parent.right = null;
      }

      return node;
    }

    if (node.left != null && node.right != null) {
      // 'node' has both children.
      T tmpKey = node.key;
      Node<T> successor = minimumNode(node.right);
      node.key = successor.key;
      Node<T> child = successor.right;
      Node<T> parent = successor.parent;

      if (parent.left == successor) {
        parent.left = child;
      } else {
        parent.right = child;
      }

      if (child != null) {
        child.parent = parent;
      }

      Node<T> lo = child;
      Node<T> hi = parent;

      while (hi != null) {
        if (hi.left == lo) {
          hi.count--;
        }

        lo = hi;
        hi = hi.parent;
      }

      successor.key = tmpKey;
      return successor;
    }

    Node<T> child;

    // 'node' has only one child.
    if (node.left != null) {
      child = node.left;
    } else {
      child = node.right;
    }

    Node<T> parent = node.parent;
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

    Node<T> hi = parent;
    Node<T> lo = child;

    while (hi != null) {
      if (hi.left == lo) {
        hi.count--;
      }

      lo = hi;
      hi = hi.parent;
    }

    return node;

  }

  private Node<T> minimumNode(Node<T> node) {
    while (node.left != null) {
      node = node.left;
    }

    return node;
  }

  private int height(Node<T> node) {
    return node == null ? -1 : node.height;
  }

  private Node<T> leftRotate(Node<T> node1) {
    Node<T> node2 = node1.right;
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

  private Node<T> rightRotate(Node<T> node1) {
    Node<T> node2 = node1.left;
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

  private Node<T> rightLeftRotate(Node<T> node1) {
    Node<T> node2 = node1.right;
    node1.right = rightRotate(node2);
    return leftRotate(node1);
  }

  private Node<T> leftRightRotate(Node<T> node1) {
    Node<T> node2 = node1.left;
    node1.left = leftRotate(node2);
    return rightRotate(node1);
  }

  // Fixing an insertion: use insertionMode = true.
  // Fixing a deletion: use insertionMode = false.
  private void fixAfterModification(Node<T> node, boolean insertionMode) {
    Node<T> parent = node.parent;
    Node<T> grandParent;
    Node<T> subTree;

    while (parent != null) {
      if (height(parent.left) == height(parent.right) + 2) {
        grandParent = parent.parent;

        if (height(parent.left.left) >= height(parent.left.right)) {
          subTree = rightRotate(parent);
        } else {
          subTree = leftRightRotate(parent);
        }

        if (grandParent == null) {
          root = subTree;
        } else if (grandParent.left == parent) {
          grandParent.left = subTree;
        } else {
          grandParent.right = subTree;
        }

        if (grandParent != null) {
          grandParent.height = Math.max(
              height(grandParent.left),
              height(grandParent.right)) + 1;
        }

        if (insertionMode) {
          // Whenever fixing after insertion, at most one rotation is
          // required in order to maintain the balance.
          return;
        }
      } else if (height(parent.right) == height(parent.left) + 2) {
        grandParent = parent.parent;

        if (height(parent.right.right) >= height(parent.right.left)) {
          subTree = leftRotate(parent);
        } else {
          subTree = rightLeftRotate(parent);
        }

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

        if (insertionMode) {
          return;
        }
      }

      parent.height = Math.max(height(parent.left),
          height(parent.right)) + 1;
      parent = parent.parent;
    }
  }

  public boolean isHealthy() {
    if (root == null) {
      return true;
    }

    return !containsCycles()
        && heightsAreCorrect()
        && isBalanced()
        && isWellIndexed();
  }

  private boolean containsCycles() {
    Set<Node<T>> visitedNodes = new HashSet<>();
    return containsCycles(root, visitedNodes);
  }

  private boolean containsCycles(Node<T> current, Set<Node<T>> visitedNodes) {
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

  private int getHeight(Node<T> node) {
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

  private boolean isBalanced(Node<T> node) {
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

  private int count(Node<T> node) {
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
}
