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


import java.util.HashSet;
import java.util.Set;


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

  public static Set<Integer> toSet(int[] bucketSet) {
    Set<Integer> resultSet;
    int arrayLength = length(bucketSet);
    if (arrayLength > 0) {
      resultSet = new HashSet(arrayLength);
      for (int i = 1; i <= arrayLength; i++) {
        resultSet.add(bucketSet[i]);
      }
    } else {
      resultSet = new HashSet();
    }
    return resultSet;
  }

  public static int[] fromSet(Set<Integer> bucketSet) {
    int setSize = bucketSet.size();
    int[] resultArray = new int[setSize + 1];
    resultArray[0] = setSize;

    if (setSize > 0) {
      int i = 1;
      for (Integer element : bucketSet) {
        resultArray[i] = element;
        i++;
      }
    }
    return resultArray;
  }

}
