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

import org.apache.geode.cache.query.internal.NullToken;
import org.apache.geode.cache.query.internal.Undefined;

/**
 * A general comparator that will let us compare different numeric types for equality
 *
 */

public class ExtendedNumericComparator extends NumericComparator implements Comparator {

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ExtendedNumericComparator;
  }

  @Override
  public int hashCode() {
    return ExtendedNumericComparator.class.hashCode();
  }

  @Override
  public int compare(Object obj1, Object obj2) {
    if (obj1.getClass() != obj2.getClass() && (obj1 instanceof Number && obj2 instanceof Number)) {
      return super.compare(obj1, obj2);
    } else if (obj2 instanceof Undefined && !(obj1 instanceof Undefined)) {
      // Everything should be greater than Undefined
      return 1;
    } else if (obj2 instanceof NullToken && !(obj1 instanceof Undefined)
        && !(obj1 instanceof NullToken)) {
      // Everything should be greater than Null except for Undefined
      return 1;
    }

    return ((Comparable) obj1).compareTo(obj2);
  }
}
