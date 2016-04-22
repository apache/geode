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
package com.gemstone.gemfire.cache.query.internal;

import java.util.Comparator;

import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * Comparator used by the sorted set for storing the results obtained from
 * evaluation of various filter operands in an increasing order of the size ,
 * which will ensure that the intersection of the results for evaluation of AND
 * junction is optimum in performance.
 * 
 */

class SelectResultsComparator implements Comparator {

  /**
   * Sort the array in ascending order of collection sizes.
   */
  public int compare(Object obj1, Object obj2) {
    if (!(obj1 instanceof SelectResults) || !(obj2 instanceof SelectResults)) {
      Support.assertionFailed("The objects need to be of type SelectResults");
    }
    int answer = -1;
    SelectResults sr1 = (SelectResults) obj1;
    SelectResults sr2 = (SelectResults) obj2;
    int sizeDifference = sr1.size() - sr2.size();
    if (obj1 == obj2) {
      answer = 0;
    }
    else if (sizeDifference > 0) {
      answer = 1;
    }
    return answer;
  }

  /**
   * Overwrite default equals implementation.
   */
  @Override
  public boolean equals(Object o1) {
    return this == o1;
  }
}
