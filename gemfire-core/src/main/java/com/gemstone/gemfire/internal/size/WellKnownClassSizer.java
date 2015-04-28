/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;


/**
 * An efficient sizer for some commonly used
 * classes.
 * 
 * This will return 0 if it does not know
 * how to size the object
 * @author dsmith
 *
 */
public class WellKnownClassSizer {
  
  private static final int BYTE_ARRAY_OVERHEAD;
  private static final int STRING_OVERHEAD;
  
  static {
    try {
      ReflectionSingleObjectSizer objSizer = new ReflectionSingleObjectSizer();
      BYTE_ARRAY_OVERHEAD = (int) objSizer.sizeof(new byte[0], false);
      STRING_OVERHEAD = (int) (ReflectionSingleObjectSizer.sizeof(String.class) + objSizer.sizeof(new char[0], false));
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static int sizeof(Object o) {
    int size = 0;
    
    if(o instanceof byte[]) {
      size =  BYTE_ARRAY_OVERHEAD + ((byte[]) o).length;
    }
    else if(o instanceof String) {
      size = STRING_OVERHEAD + ((String) o).length() * 2; 
    } else {
      return 0;
    }
    
    size = (int) ReflectionSingleObjectSizer.roundUpSize(size);
    return size;
  }

}
