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

package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.geode.cache.Region;

public class TestData {
  public static void populateRegion(Region region, int numValues) {
    IntStream.rangeClosed(1, numValues).forEach(i -> region.put(i, new MyValue(i)));
  }

  public static Set<Integer> createAndPopulateSet(int nBuckets) {
    return new HashSet<Integer>(IntStream.range(0, nBuckets).boxed().collect(Collectors.toSet()));
  }

  public static class MyValue implements Serializable, Comparable<MyValue> {
    public int value = 0;

    public MyValue(int value) {
      this.value = value;
    }

    public int compareTo(MyValue o) {
      if (this.value > o.value) {
        return 1;
      } else if (this.value < o.value) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
