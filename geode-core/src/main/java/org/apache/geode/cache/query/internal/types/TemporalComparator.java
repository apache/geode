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
 * Comparator for mixed comparisons between instances of java.util.Date, java.sql.Date,
 * java.sql.Time, and java.sql.Timestamp.
 *
 * @version $Revision: 1.1 $
 */


class TemporalComparator implements Comparator {
  // all temporal comparators are created equal
  @Override
  public boolean equals(Object obj) {
    return obj instanceof TemporalComparator;
  }

  @Override
  public int hashCode() {
    return TemporalComparator.class.hashCode();
  }


  // throws ClassCastExcepton if obj1 or obj2 is not a java.util.Date or subclass
  @Override
  public int compare(Object obj1, Object obj2) {
    java.util.Date date1 = (java.util.Date) obj1;
    java.util.Date date2 = (java.util.Date) obj2;
    long ms1 = date1.getTime();
    long ms2 = date2.getTime();

    // if we're dealing with Timestamps, then we need to extract milliseconds
    // out of the nanos and then do a compare with the "extra" nanos
    int extraNanos1 = 0;
    int extraNanos2 = 0;
    if (date1 instanceof java.sql.Timestamp) {
      int nanos = ((java.sql.Timestamp) date1).getNanos();
      int ms = nanos / 1000000;
      ms1 += ms;
      extraNanos1 = nanos - (ms * 1000000);
    }

    if (date2 instanceof java.sql.Timestamp) {
      int nanos = ((java.sql.Timestamp) date2).getNanos();
      int ms = nanos / 1000000;
      ms2 += ms;
      extraNanos2 = nanos - (ms * 1000000);
    }

    if (ms1 != ms2) {
      return ms1 < ms2 ? -1 : 1;
    }
    return extraNanos1 == extraNanos2 ? 0 : (extraNanos1 < extraNanos2 ? -1 : 1);
  }
}
