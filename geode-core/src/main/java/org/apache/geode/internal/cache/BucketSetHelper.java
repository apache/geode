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
package org.apache.geode.internal.cache;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

public class BucketSetHelper {
  public static int get(int[] bucketSet, int index) {
    return bucketSet[index + 1];
  }

  public static int length(int[] bucketSet) {
    return null == bucketSet || bucketSet.length < 2 ? 0 : bucketSet[0];
  }

  public static void add(int[] bucketSet, int value) {
    int index = bucketSet[0] + 1;
    bucketSet[index] = value;
    bucketSet[0] = index;
  }

  public static Set<Integer> toSet(int[] bucketArray) {
    Set<Integer> bucketSet;
    if (BucketSetHelper.length(bucketArray) > 0) {
      bucketSet =
          new HashSet(
              Arrays.asList(
                  ArrayUtils.toObject(Arrays.copyOfRange(bucketArray, 1, bucketArray[0] + 1))));
    } else {
      bucketSet = new HashSet();
    }
    return bucketSet;
  }

  public static int[] fromSet(Set<Integer> bucketSet) {
    int[] bucketArray = new int[bucketSet.size() + 1];
    bucketArray[0] = bucketSet.size();

    if (bucketSet.size() > 0) {
      System.arraycopy(ArrayUtils.toPrimitive(bucketSet.toArray(new Integer[bucketSet.size()])), 0,
          bucketArray,
          1, bucketSet.size());
    }
    return bucketArray;
  }

}
