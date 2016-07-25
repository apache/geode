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
package com.examples;

import java.util.*;

/**
 * The super class of another class.  Neither is
 * <code>Serializable</code>. 
 *
 *
 * @since GemFire 3.5
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
