/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Basic Stack implementation, used by
 * {@link PreprocessorUtils#isSyntaxValid(String)} for detecting valid syntax
 * 
 * @author Nikhil Jadhav
 * 
 * @param <T>
 */
public class Stack<T> {
  private List<T> list = new ArrayList<T>();

  public void push(T object) {
    list.add(object);
  }

  public T pop() {
    if (list.size() > 0) {
      int length = list.size();
      T object = list.get(length - 1);
      list.remove(length - 1);
      return object;
    } else {
      return null;
    }
  }

  public Boolean isEmpty() {
    if (list.size() == 0) {
      return true;
    } else {
      return false;
    }
  }
}
