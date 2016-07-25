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
package com.gemstone.gemfire.cache.util;

/**
 * 
 * From 9.0 old wan support is removed. Ideally Gateway (used for old wan) should be removed but it it there for 
 * rolling upgrade support when GatewaySenderProfile update request comes from or sent to old member.
 * Old member uses Gateway$OrderPolicy while latest members uses GatewaySender#OrderPolicy
 * 
 * @since Geode 1.0
 *
 */
public class Gateway {

  /**
   * The order policy. This enum is applicable only when concurrency-level is > 1.
   * 
   * @since GemFire 6.5.1
   */
  public enum OrderPolicy {
    /**
     * Indicates that events will be parallelized based on the event's
     * originating member and thread
     */
    THREAD,
    /**
     * Indicates that events will be parallelized based on the event's key
     */
    KEY,
    /** Indicates that events will be parallelized based on the event's:
     *  - partition (using the PartitionResolver) in the case of a partitioned
     *    region event
     *  - key in the case of a replicated region event
     */
    PARTITION
  }
}
