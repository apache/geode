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
package com.examples;

/**
 * A node in a linked list that is used to test that serializing
 * non-<code>Serializable</code> objects handle back references
 * correctly. 
 *
 *
 * @since GemFire 3.5
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
