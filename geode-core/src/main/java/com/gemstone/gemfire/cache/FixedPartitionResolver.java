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
package com.gemstone.gemfire.cache;

import java.util.Set;

/**
 * Implementers of interface <code>FixedPartitionResolver</code> helps to
 * achieve explicit mapping of a "user defined" partition to a data member node.
 * <p>
 * GemFire uses the partition name returned by {@link FixedPartitionResolver#getPartitionName(EntryOperation, Set)} to determine on
 * which member the data is being managed. Say, for example, you want to
 * partition all Trades according to quarters. You can implement
 * FixedPartitionResolver to get the name of the quarter based on the date given
 * as part of {@link com.gemstone.gemfire.cache.EntryOperation}.
 * </p>
 * 
 * public class QuarterPartitionResolver implements FixedPartitionResolver{<br>
 * &nbsp &nbsp public String getPartitionName(EntryOperation opDetails, Set
 * allAvailablePartitions) {<br>
 * &nbsp &nbsp Date date = sdf.parse((String)opDetails.getKey());<br>
 * &nbsp &nbsp Calendar cal = Calendar.getInstance();<br>
 * &nbsp &nbsp cal.setTime(date);<br>
 * &nbsp &nbsp int month = cal.get(Calendar.MONTH);<br>
 * &nbsp &nbsp if (month == 0 || month == 1 || month == 2) {<br>
 * &nbsp &nbsp &nbsp return "Quarter1";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 3 || month == 4 || month == 5) {<br>
 * &nbsp &nbsp &nbsp return "Quarter2";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 6 || month == 7 || month == 8) {<br>
 * &nbsp &nbsp &nbsp return "Quarter3";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 9 || month == 10 || month == 11) {<br>
 * &nbsp &nbsp &nbsp return "Quarter4";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else {<br>
 * &nbsp &nbsp &nbsp return "Invalid Quarter";<br>
 * &nbsp &nbsp }<br>
 * &nbsp }<br>
 *
 * @see PartitionResolver
 * @since GemFire 6.6
 *
 * 
 */
public interface FixedPartitionResolver<K, V> extends PartitionResolver<K, V> {

  /**
   * This method is used to get the name of the partition for the given entry
   * operation.
   * 
   * @param opDetails
   *          the details of the entry operation e.g. {@link Region#get(Object)}
   * @param targetPartitions
   *           Avoid using this parameter.This set is deprecated from 8.0 and same will be removed in future release.
   *           Represents all the available primary partitions on the nodes.
   * 
   * @return partition-name associated with node which allows mapping of given
   *         data to user defined partition
   */
  public String getPartitionName(EntryOperation<K, V> opDetails,
      @Deprecated Set<String> targetPartitions);
}
