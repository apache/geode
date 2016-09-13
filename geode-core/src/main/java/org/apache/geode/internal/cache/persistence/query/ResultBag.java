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
package org.apache.geode.internal.cache.persistence.query;

import org.apache.geode.internal.cache.CachedDeserializable;

/**
 * The contract for a sorted bag of temporary results for a query.
 * This bag may be persisted on disk.
 * 
 * This class is threadsafe. Iterators will reflect all entries added to
 * the set up until the time that the iterator was obtained. After that they
 * may or may not reflect modifications to the set while the iteration is in progress.
 * They will guarantee that entries will be returned in the correct order.
 * 
 * The element is allowed to be an object, or a CachedDeserializable.
 * 
 * If multiple instances of the same element are required, use SortedResultBag
 * instead.
 * 
 * The result set should be closed using the close method to free up resources
 * when it is no longer in use.
 * 
 * @since GemFire cedar
 */
public interface ResultBag {

  /**
   * Add an element to the bag. Multiple copies of the same
   * entry may be inserted into the bag.
   * @param e the element. e may be NULL.
   */
  void add(Object e);

  /**
   * Return all of the elements in the set.
   */
  CloseableIterator<CachedDeserializable> iterator();
  
  /**
   * Close the result set and free up any resources on disk
   * associated with the result set.
   */
  public void close();
}
