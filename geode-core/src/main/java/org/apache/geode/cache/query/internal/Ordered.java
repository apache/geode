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
package org.apache.geode.cache.query.internal;

import java.util.Comparator;

/**
 *
 * This interface is to be implemented by all the query SelectResults implementation which have ordered
 * data. This encompasses those classes which have data stored in a List, LinkedMap, LinkedSet, TreeMap
 * , TreeSet etc.
 * @see NWayMergeResults
 * @see SortedResultsBag
 * @see SortedStructBag
 * @see SortedStructSet
 * @see SortedResultSet
 * @see LinkedResultSet
 * @see LinkedStructSet
 *
 */
public interface Ordered {
  Comparator comparator(); 
  
  //Implies that underlying structure is a LinkedHashMap or LinkedHashSet & the structs are stored
  // directly , ie not in terms of Object[]
  // SortedResultsBag, LinkedResultSet are two such types.
  
  boolean dataPreordered();
}
