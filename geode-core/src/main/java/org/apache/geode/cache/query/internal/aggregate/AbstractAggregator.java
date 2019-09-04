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
package org.apache.geode.cache.query.internal.aggregate;

import org.apache.geode.cache.query.Aggregator;

/**
 * Abstract Aggregator class providing support for down casting the result.
 */
public abstract class AbstractAggregator implements Aggregator {

  public static Number downCast(long value) {
    Number retVal;

    if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
      retVal = Integer.valueOf((int) value);
    } else {
      retVal = value;
    }

    return retVal;
  }

  public static Number downCast(double value) {
    Number retVal;

    if (value % 1 == 0) {
      retVal = downCast((long) value);
    } else {
      if (value <= Float.MAX_VALUE && value >= Float.MIN_VALUE) {
        retVal = Float.valueOf((float) value);
      } else {
        retVal = Double.valueOf(value);
      }
    }

    return retVal;
  }
}
