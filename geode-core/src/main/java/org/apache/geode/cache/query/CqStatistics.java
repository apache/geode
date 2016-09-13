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

package com.gemstone.gemfire.cache.query;

/**
 * This class provides methods to get statistical information about a registered Continuous Query (CQ)
 * represented by the CqQuery object. 
 * 
 * @since GemFire 5.5
 */
public interface CqStatistics {

  /**
   * Get number of Insert events qualified by this CQ.
   * @return long number of inserts.
   */
  public long numInserts();
  
  /**
   * Get number of Delete events qualified by this CQ.
   * @return long number of deletes.
   */
  public long numDeletes();
  
  /**
   * Get number of Update events qualified by this CQ.
   * @return long number of updates.
   */
  public long numUpdates();

  /**
   * Get total of all the events qualified by this CQ.
   * @return long total number of events.
   */
  public long numEvents();

}
