/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples;

/**
 * A node in a linked list that is used to test that serializing
 * non-<code>Serializable</code> objects handle back references
 * correctly. 
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class LinkNode {

  /** The value of this LinkNode */
  private int value;

  /** An object value in this LinkNode */
  public Object object;

  /** The next node in the chain */
  public LinkNode next;

  /**
   * Creates a new <code>LinkNode</code> with the given value
   */
  public LinkNode(int value) {
    this.value = value;
  }

  /**
   * Two <code>LinkNode</code>s are equal if they have the same
   * <code>value</code> and their <code>next</code> node have the same
   * value.
   */
  public boolean equals(Object o) {
    if (!(o instanceof LinkNode)) {
      return false;
    }

    LinkNode other = (LinkNode) o;
    if (this.value != other.value) {
      return false;

    } else if (this.next == null) {
      return other.next == null;

    } else if (other.next == null) {
      return this.next == null;

    } else if (other.next.value != this.next.value) {
      return false;

    } else if (this.object != null) {
      return this.object.equals(other.object);

    } else {
      return other.object == null;
    }

  }

}
