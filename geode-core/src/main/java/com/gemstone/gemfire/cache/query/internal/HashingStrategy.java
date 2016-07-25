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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import java.io.Serializable;

/**
 * This interface defines a Hashing strategy for keys(OR values)
 * in a HashMap(OR HashSet) for calculation of hash-code for
 * custom objects and primitive types.
 * 
 * @since GemFire 8.0
 *
 */
public interface HashingStrategy extends Serializable {

  /**
   * Computes a hash code for the specified object.  Implementors
   * can use the object's own <tt>hashCode</tt> method, the Java
   * runtime's <tt>identityHashCode</tt>, or a custom scheme.
   * 
   * @param o object for which the hash-code is to be computed
   * @return the hashCode
   */
  public int hashCode(Object o);

  /**
   * Compares o1 and o2 for equality.  Strategy implementors may use
   * the objects' own equals() methods, compare object references,
   * or implement some custom scheme.
   *
   * @param o1 an <code>Object</code> value
   * @param o2 an <code>Object</code> value
   * @return true if the objects are equal according to this strategy.
   */
  public boolean equals(Object o1, Object o2);
}
