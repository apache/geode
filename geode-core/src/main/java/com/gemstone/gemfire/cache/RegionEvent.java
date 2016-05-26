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

/** Contains information about an event affecting a region, including
 * its identity and the circumstances of the event.
 * This is passed in to <code>CacheListener</code> and <code>CacheWriter</code>.
 *
 *
 *
 * @see CacheListener
 * @see CacheWriter
 * @see EntryEvent
 * @since GemFire 2.0
 */
public interface RegionEvent<K,V> extends CacheEvent<K,V> {
  
  /**
   * Return true if this region was destroyed but is being reinitialized,
   * for example if a snapshot was just loaded. Can only return true for
   * an event related to region destruction.
   */
  public boolean isReinitializing();
  
}
