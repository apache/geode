/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: NumericComparator.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal.types;

import java.util.*;

import com.gemstone.gemfire.cache.query.internal.NullToken;
import com.gemstone.gemfire.cache.query.internal.Undefined;

/**
 * A general comparator that will let us compare different numeric types for equality
 * 
 * @author jhuynh
 */

public class ExtendedNumericComparator extends NumericComparator implements Comparator {
  
  @Override
  public boolean equals(Object obj) {
    return obj instanceof ExtendedNumericComparator;
  }

  public int compare(Object obj1, Object obj2) {
    if (obj1.getClass() != obj2.getClass()
        && (obj1 instanceof Number && obj2 instanceof Number)) {
      return super.compare(obj1, obj2);
    } else if(obj2 instanceof Undefined && !(obj1 instanceof Undefined)){
      // Everthing should be greater than Undefined
      return 1;
    }else if(obj2 instanceof NullToken && !(obj1 instanceof NullToken)){
      // Everthing should be greater than Null
      return 1;
    }

    return ((Comparable) obj1).compareTo(obj2);
  }
}
