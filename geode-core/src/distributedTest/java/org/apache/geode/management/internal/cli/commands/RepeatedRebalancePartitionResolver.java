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
package org.apache.geode.management.internal.cli.commands;


import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;

// This PartitionResolver deliberately assigns entries to buckets in the partitioned regions
// in RepeatedRebalanceDUnitTest to artificially produce buckets with unbalanced data distribution

public class RepeatedRebalancePartitionResolver implements PartitionResolver {

  @Override
  public Object getRoutingObject(EntryOperation opDetails) {
    String key = (String) opDetails.getKey();

    // This partition resolver requires keys to be Strings starting with the letters "key" followed
    // by an integer
    int number = Integer.valueOf(key.substring(3));

    // The partitioned regions that use this partition resolver in RepeatedRebalanceDUnitTest have
    // 48 buckets.
    // In order to create an unbalanced distribution of data between the buckets, for every 108
    // entries,
    // one of three "big buckets" (associated with the values 0, 1 and 2) are each assigned an
    // additional 20 entries.
    int mod = number % 108;
    if (mod > 48) {
      mod = number % 3;
    }
    return mod;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }
}
