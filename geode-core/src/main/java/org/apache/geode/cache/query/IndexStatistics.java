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
package org.apache.geode.cache.query;

//import java.util.*;

/**
 * Provides statistics about a GemFire cache {@link Index}.
 *
 * @since GemFire 4.0
 */
public interface IndexStatistics {

  /** Return the total number of times this index has been updated
   */
  public long getNumUpdates();
  
  /** Returns the total amount of time (in nanoseconds) spent updating
   *  this index.
   */
  public long getTotalUpdateTime();
  
  /**
   * Returns the total number of times this index has been accessed by
   * a query.
   */
  public long getTotalUses();

  /**
   * Returns the number of keys in this index.
   */
  public long getNumberOfKeys();

  /**
   * Returns the number of values in this index.
   */
  public long getNumberOfValues();


  /** Return the number of values for the specified key
   *  in this index.
   */
  public long getNumberOfValues(Object key);

  
  /**
   * Return number of read locks taken on this index
   */
  public int getReadLockCount();

  /**
   * Returns the number of keys in this index
   * at the highest level
   */
  public long getNumberOfMapIndexKeys();
  
  /**
   * Returns the number of bucket indexes created
   * in the Partitioned Region
   */
  public int getNumberOfBucketIndexes();
}
