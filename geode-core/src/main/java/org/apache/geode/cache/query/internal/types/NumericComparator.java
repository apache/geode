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

package org.apache.geode.cache.query.internal.types;

import java.util.Comparator;


/**
 * Comparator for mixed comparisons between numbers.
 *
 * @version $Revision: 1.1 $
 */


class NumericComparator implements Comparator {
  // all numeric comparators are created equal
  @Override
  public boolean equals(Object obj) {
    return obj instanceof NumericComparator;
  }

  @Override
  public int hashCode() {
    return NumericComparator.class.hashCode();
  }


  // throws ClassCastExcepton if obj1 or obj2 is not a Number
  @Override
  public int compare(Object obj1, Object obj2) {
    Number num1 = (Number) obj1;
    Number num2 = (Number) obj2;


    // create as few objects as possible, but
    // use compareTo algorithm in the wrapper classes for float
    // and double so it has the same behavior as index lookups.
    // for integers, the algorithm is the same for wrappers and primitives,
    // so use primitives there.
    if (num1 instanceof Double) {
      if (num2 instanceof Double)
      // this case may happen if one is a Double.TYPE
      // and the other is Double.class
      {
        return ((Double) num1).compareTo((Double) num2);
      } else {
        return ((Double) num1).compareTo(num2.doubleValue());
      }
    } else if (num2 instanceof Double) {
      return Double.valueOf(num1.doubleValue()).compareTo((Double) num2);
    }

    if (num1 instanceof Float) {
      if (num2 instanceof Float) {
        return ((Float) num1).compareTo((Float) num2);
      } else {
        return ((Float) num1).compareTo((float) num2.doubleValue());
      }
    } else if (num2 instanceof Float) {
      return new Float(num1.doubleValue()).compareTo((Float) num2);
    }


    if (num1 instanceof Long) {
      if (num2 instanceof Long) {
        return ((Long) num1).compareTo((Long) num2);
      } else {
        long l1 = num1.longValue();
        long l2 = num2.longValue();
        return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
      }
    } else if (num2 instanceof Long) {
      long l1 = num1.longValue();
      long l2 = num2.longValue();
      return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
    }

    // ints
    int i1 = num1.intValue();
    int i2 = num2.intValue();
    return i1 == i2 ? 0 : (i1 < i2 ? -1 : 1);
  }
}
