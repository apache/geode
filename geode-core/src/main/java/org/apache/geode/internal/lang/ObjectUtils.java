/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.lang;

/**
 * The ObjectUtils class is an abstract utility class for working with and invoking methods on
 * Objects.
 * <p>
 *
 * @see java.lang.Object
 * @since GemFire 6.8
 */
@SuppressWarnings("unused")
public abstract class ObjectUtils {

  /**
   * Determines whether 2 Objects are equal in value. The Objects are equal if and only if neither
   * are null and are equal according to the equals method of the Object's class type.
   * <p>
   *
   * @param obj1 the first Object in the equality comparison.
   * @param obj2 the second Object in the equality comparison.
   * @return a boolean value indicating whether the 2 Objects are equal in value.
   * @see java.lang.Object#equals(Object)
   */
  public static boolean equals(final Object obj1, final Object obj2) {
    return (obj1 != null && obj1.equals(obj2));
  }

  /**
   * Determines whether 2 Objects are equal in value by ignoring nulls. If both Object references
   * are null, then they are considered equal, or neither must be null and the Objects must be equal
   * in value as determined by their equals method.
   * <p>
   *
   * @param obj1 the first Object in the equality comparison.
   * @param obj2 the second Object in the equality comparison.
   * @return a boolean value indicating whether the 2 Objects are equal in value. If both Object
   *         references are null, then they are considered equal.
   * @see java.lang.Object#equals(Object)
   */
  public static boolean equalsIgnoreNull(final Object obj1, final Object obj2) {
    return (obj1 == null ? obj2 == null : obj1.equals(obj2));
  }

  /**
   * A null-safe computation of the specified Object's hash value. If the Object reference is null,
   * then this method returns 0 and will be consistent with the equalsIgnoreNull equality
   * comparison.
   * <p>
   *
   * @param obj the Object who's hash value will be computed.
   * @return an integer signifying the hash value of the Object or 0 if the Object reference is
   *         null.
   * @see java.lang.Object#hashCode()
   */
  public static int hashCode(final Object obj) {
    return (obj == null ? 0 : obj.hashCode());
  }

  /**
   * Null-safe implementation of the Object.toString method.
   * <p>
   *
   * @param obj the Object on which to call toString.
   * @return the String representation of the specified Object or null if the Object reference is
   *         null.
   * @see java.lang.Object#toString()
   */
  public static String toString(final Object obj) {
    return (obj == null ? null : obj.toString());
  }

}
