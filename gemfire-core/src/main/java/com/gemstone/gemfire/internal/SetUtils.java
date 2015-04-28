/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Utility methods for managing and using Sets 
 * @author Mitch Thomas
 * @since gemfire59poc
 * 
 */
public final class SetUtils {
  public static  <T> boolean intersectsWith(final Set<? extends T> a, final Set<? extends T> b) {
    if (a == b) {
      return true;
    }
    final Set/*<T>*/ lSet, sSet;
    if (a.size() >= b.size()) {
      lSet = a; sSet = b;
    } else {
      lSet = b; sSet = a;
    }
    for (Iterator i=sSet.iterator(); i.hasNext(); ) {
      Object item = i.next();
      if (lSet.contains(item)) {
        return true;
      }
    }
    return false;
  }
  
  public static /*T*/ Set/*<T>*/ intersection(final Set/*<T>*/ a, final Set/*<T>*/ b) {
    if (a == b) {
      return a;
    }
    final Set/*<T>*/ lSet, sSet;
    if (a.size() >= b.size()) {
      lSet = a; sSet = b;
    } else {
      lSet = b; sSet = a;
    }
    HashSet /*<T>*/ ret = new HashSet/*<T>*/();
    for (Iterator i=sSet.iterator(); i.hasNext(); ) {
      Object item = i.next();
      if (lSet.contains(item)) {
        ret.add(item);
      }
    }
    return ret;
  }
}
