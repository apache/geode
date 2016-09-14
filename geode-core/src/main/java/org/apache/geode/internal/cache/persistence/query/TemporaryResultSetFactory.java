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

import org.apache.geode.internal.cache.persistence.query.mock.ResultListImpl;
import org.apache.geode.internal.cache.persistence.query.mock.SortedResultBagImpl;
import org.apache.geode.internal.cache.persistence.query.mock.SortedResultSetImpl;

/**
 * This is a factory for temporary result sets that overflow to disk.
 * 
 * The result sets will not be recovered when the member restarts.
 * Any temporary results still on disk when a member restarts will be deleted.
 *
 */
public class TemporaryResultSetFactory {
  
  
  /**
   * Get a result set that is sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * @param extractor a callback to extract the index sort key from
   * the object. The sort key is expected to be comparable.
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultSet getSortedResultSet(SortKeyExtractor extractor, boolean reverse) {
    return new SortedResultSetImpl(extractor, reverse);
  }
  
  /**
   * Get a result bag that is sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * @param extractor a callback to extract the index sort key from
   * the object. The sort key is expected to be comparable.
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultBag getSortedResultBag(SortKeyExtractor extractor, boolean reverse) {
    return new SortedResultBagImpl(extractor, reverse);
  }
  
  /**
   * Get a result set that is not sorted. The result set will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   * This is useful for cases where the ordering is not important,
   * but the set semantics are. For example, a distinct query.
   * 
   * @param reverse - true to reverse the natural order of the keys
   */
  public ResultSet getUnsortedResultSet(boolean reverse) {
    return new SortedResultSetImpl(null, reverse);
  }
  
  /**
   * Get a list to store temporary results. The list will be overflowed
   * on to disk as necessary, but it will not be recovered from disk.
   * 
   */
  public ResultList getResultList() {
    return new ResultListImpl();
  }
}
