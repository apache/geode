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
package org.apache.geode.cache.query;


/**
 * Provides statistics about a GemFire cache {@link Index}.
 *
 * @since GemFire 4.0
 */
public interface IndexStatistics {

  /**
   * Return the total number of times this index has been updated
   *
   * @return the total number of times this index has been updated
   */
  long getNumUpdates();

  /**
   * Returns the total amount of time (in nanoseconds) spent updating this index.
   *
   * @return the total amount of time (in nanoseconds) spent updating this index
   */
  long getTotalUpdateTime();

  /**
   * Returns the total number of times this index has been accessed by a query.
   *
   * @return the total number of times this index has been accessed by a query
   */
  long getTotalUses();

  /**
   * Returns the number of keys in this index.
   *
   * @return the number of keys in this index
   */
  long getNumberOfKeys();

  /**
   * Returns the number of values in this index.
   *
   * @return the number of values in this index
   */
  long getNumberOfValues();


  /**
   * Return the number of values for the specified key in this index.
   *
   * @param key the key
   * @return the number of values for the specified key in this index
   */
  long getNumberOfValues(Object key);


  /**
   * Return number of read locks taken on this index
   *
   * @return the number of read locks taken on this index
   *
   * @deprecated Use {@link #getReadLockCountLong()}
   */
  @Deprecated
  int getReadLockCount();

  /**
   * Return number of read locks taken on this index
   *
   * @return the number of read locks taken on this index
   */
  long getReadLockCountLong();


  /**
   * Returns the number of keys in this index at the highest level
   *
   * @return the number of keys in this index at the highest level
   */
  long getNumberOfMapIndexKeys();

  /**
   * Returns the number of bucket indexes created in the Partitioned Region
   *
   * @return the number of bucket indexes created in the Partitioned Region
   *
   * @deprecated Use {@link #getNumberOfBucketIndexesLong()}
   */
  @Deprecated
  int getNumberOfBucketIndexes();

  /**
   * Returns the number of bucket indexes created in the Partitioned Region
   *
   * @return the number of bucket indexes created in the Partitioned Region
   */
  long getNumberOfBucketIndexesLong();
}
