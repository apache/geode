/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples;

import java.util.*;

/**
 * The super class of another class.  Neither is
 * <code>Serializable</code>. 
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class SuperClass {

  protected int intValue;
  protected HashMap map;

  /**
   * Creates a new <code>SuperClass</code>
   */
  protected SuperClass() {
    this.intValue = 42;
    this.map = new HashMap();
    map.put("one", new Integer(1));
    map.put("two", new Integer(2));
    map.put("three", new Integer(3));
    map.put("four", new Integer(4));
  }


  public static class SubClass extends SuperClass {

    protected Map anotherMap;
    protected long longValue;

    /**
     * Creates a new <code>SubClass</code>
     */
    public SubClass() {
      super();

      this.longValue = 28L;
      this.anotherMap = new HashMap();
      this.anotherMap.put("five", new Integer(5));
      this.anotherMap.put("six", new Integer(6));
      this.anotherMap.put("seven", new Integer(7));
      this.anotherMap.put("eight", new Integer(8));
    }

    public boolean equals(Object o) {
      if (!(o instanceof SubClass)) {
        return false;
      }

      SubClass other = (SubClass) o;
      if (this.intValue != other.intValue) {
        return false;

      } else if (!this.map.equals(other.map)) {
        return false;

      } else if (this.longValue != other.longValue) {
        return false;

      } else if (!this.anotherMap.equals(other.anotherMap)) {
        return false;

      } else {
        return true;
      }
    }

  }



}
