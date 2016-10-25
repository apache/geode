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

package org.apache.geode.internal.admin;

/**
 * Represents an arbitrary object that has been placed into a GemFire
 * <code>Region</code>.
 */
public interface EntryValueNode {

  /**
   * Returns true if this node represents a primitive value or String
   */
  public boolean isPrimitiveOrString();

  /**
   * Returns the field name, if any
   */
  public String getName();

  /**
   * Returns the class name
   */
  public String getType();

  /**
   * Returns the fields in physical inspection, or the logical elements
   * in logical inspection
   */
  public EntryValueNode[] getChildren();

  /**
   * Returns the wrapped primitive value if this is a primitive
   * or the result of calling <code>toString()</code> if this is
   * an object.
   */
  public Object getPrimitiveValue();
}
