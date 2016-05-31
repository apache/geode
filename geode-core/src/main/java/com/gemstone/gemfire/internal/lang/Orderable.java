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

package com.gemstone.gemfire.internal.lang;

/**
 * The Orderable interface defines a contract for classes whose Objects can be sorted, or ordered according to the
 * order property of a Comparable type.
 * <p/>
 * @see java.lang.Comparable
 * @since GemFire 6.8
 */
public interface Orderable<T extends Comparable<T>> {

  /**
   * Returns the value of the order property used in ordering instances of this implementing Object relative to other
   * type compatible Object instances.  The order property value can also be used in sorting operations, or in
   * maintaining Orderable objects in ordered data structures like arrays or Lists, or even for defining a precedence
   * not directly related order.
   * <p/>
   * @return a value that is Comparable to other value of the same type and defines the relative order of this Object
   * instance to it's peers.
   */
  public T getOrder();

}
