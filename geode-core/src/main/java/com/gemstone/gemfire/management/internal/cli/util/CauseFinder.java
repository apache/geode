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
package com.gemstone.gemfire.management.internal.cli.util;

/**
 * Helper class to navigate through the nested causes of an exception. 
 * Provides method to
 * <ul>
 * <li> find index of an Exception of a specific type
 * <li> find Root Cause of the Exception
 * <li> retrieve a nested cause at a specific index/depth
 * <li> find a cause by specific type 
 * </ul>
 * 
 * @since GemFire 7.0
 */
public class CauseFinder {
  private static final int START_INDEX = 0;

  /**
   * Returns the index of a first nested cause which is of the given Throwable
   * type or its sub-type depending on <code>isSubtypeOk</code> value. <p /> 
   * NOTE: It looks for the "nested" causes & doesn't try to match the
   * <code>causeClass</code> of the <code>parent</code>. <p />
   * 
   * Returns -1 if a cause with the given Throwable type is not found.
   * 
   * @param parent
   *          parent Throwable instance
   * @param causeClass
   *          type of the nested Throwable cause
   * @param isSubtypeOk
   *          whether any matching sub-type is required or exact match is
   *          required
   * @return index/depth of the cause starting from the 'top most' Throwable in
   *         the stack. -1 if can't find it.
   */
  public static int indexOfCause(Throwable parent, Class<? extends Throwable> causeClass, final boolean isSubtypeOk) {
    return indexOfCause(parent, causeClass, START_INDEX, isSubtypeOk);
  }
  
  /**
   * Returns the index of a first nested cause which is of the given Throwable
   * type or its sub-type depending on <code>isSubtypeOk</code> value. <p /> 
   * NOTE: It looks for the "nested" causes & doesn't try to match the
   * <code>causeClass</code> of the <code>parent</code>. <p />
   * 
   * Returns -1 if a cause with the given Throwable type is not found.
   * 
   * @param parent
   *          parent Throwable instance
   * @param causeClass
   *          type of the nested Throwable cause
   * @param cindex
   *          variable to store current index/depth of the cause
   * @param isSubtypeOk
   *          whether any matching sub-type is required or exact match is
   *          required
   * @return index/depth of the cause starting from the 'top most' Throwable
   *         in the stack. -1 if can't find it.
   */
  private static int indexOfCause(Throwable parent, Class<? extends Throwable> causeClass, final int cindex, final boolean isSubtypeOk) {
    int resultIndex = cindex;
    Throwable cause = parent.getCause();

    //(cause is not null & cause is not of type causeClass)
    if (cause != null && !isMatching(cause, causeClass, isSubtypeOk)) {
      //recurse deeper
      resultIndex = indexOfCause(cause, causeClass, cindex + 1, isSubtypeOk);
    } else if (cause != null && isMatching(cause, causeClass, isSubtypeOk)) {
      resultIndex = cindex != -1 ? cindex + 1 : cindex;
    } else if (cause == null) {
      resultIndex = -1;
    }
    
    return resultIndex;
  }
  
  /**
   * Returns whether the given <code>cause</code> is assignable or has same
   * type as that of <code>causeClass</code> depending on
   * <code>isSubtypeOk</code> value.
   * 
   * @param cause
   *          parent Throwable instance
   * @param causeClass
   *          type of the nested Throwable cause
   * @param isSubtypeOk
   *          whether any matching sub-type is required or exact match is
   *          required
   * @return true if <code>cause</code> is assignable or has same type as that
   *         of <code>causeClass</code>. false otherwise
   */
  //Note: No not null check on 'Throwable cause' - currently called after null check 
  private static boolean isMatching(Throwable cause, Class<? extends Throwable> causeClass, final boolean isSubtypeOk) {
    return isSubtypeOk ? causeClass.isInstance(cause) : causeClass == cause.getClass();
  }
  
  /**
   * Returns nested 'root' cause of the given parent Throwable. Will return the
   * same Throwable if it has no 'cause'.
   * 
   * @param parent
   *          Throwable whose root cause is to be found
   * @return nested root cause.
   * @throws IllegalArgumentException
   *           when parent is <code>null</code>
   */
  public static Throwable getRootCause(Throwable parent) {
    return getRootCause(parent, 0);
  }

  /**
   * Returns nested 'root' cause of the given parent Throwable. Will return the
   * same Throwable if it has no 'cause'. If <code>depth</code> is 0, does a
   * <code>null</code> check on the given <code>parent Throwable</code> & if
   * <code>parent</code> is <code>null</code>, throws
   * {@link IllegalArgumentException}.
   * 
   * @param parent
   *          Throwable whose root cause is to be found
   * @param depth
   *          recurse depth indicator
   * @return nested root cause.
   * @throws IllegalArgumentException
   *           when parent is <code>null</code> at <code>depth = 0</code>
   */
  //Note: private method
  private static Throwable getRootCause(Throwable parent, int depth) {
    if (depth == 0) {
      if (parent == null) {
        throw new IllegalArgumentException("Given parent Throwable is null.");
      }
    }
    Throwable theCause = parent.getCause();
    if (theCause != null) {
      //recurse deeper
      return getRootCause(theCause, depth + 1);
    }

    return parent;
  }
  
  /**
   * Returns cause at the specified depth/index starting from the 'top most'
   * Throwable <code>parent</code> in the stack. Returns <code>null</code> if
   * it can't find it.
   * 
   * @param parent
   *          Throwable to use to find the cause at given index/depth
   * @param requiredIndex
   *          index/depth of nesting the cause
   * @return cause at the specified index starting from the 'top most'
   *         Throwable in the stack. <code>null</code> if it can't find it.
   */
  public static Throwable causeAt(Throwable parent, final int requiredIndex) {
    //
    // TODO:
    //  NEED TO CHECK WHETHER RIGHT PARENT CAUSE RETRUNED
    //
    if (parent != null && requiredIndex > 0) {
      //recurse deeper
      return causeAt(parent.getCause(), requiredIndex - 1);
    }
    
    //when there isn't required depth
    if (requiredIndex > 0) {
      return null;
    }

    return parent;
  }
  
  /**
   * Returns the first occurrence of nested cause of <code>parent</code> which
   * matches the specified <code>causeType</code> or its sub-type depending on
   * <code>isSubtypeOk</code> value.
   * 
   * @param parent
   *          Throwable to use to find the cause of given type
   * @param causeType
   *          type of the nested Throwable cause
   * @param isSubtypeOk
   *          whether any matching sub-type is required or exact match is
   *          required
   * @return the first occurrence of nested cause which matches the specified
   *         <code>causeType</code> or its sub-type. <code>null</code> if it
   *         can't find it.
   */
  public static Throwable causeByType(Throwable parent, Class<? extends Throwable> causeType, boolean isSubtypeOk) {
    Throwable cause = null;
    int foundAtIndex = indexOfCause(parent, causeType, isSubtypeOk);
    if (foundAtIndex != -1) {
      cause = causeAt(parent, foundAtIndex);
    }
    return cause;
  }
}

