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

package com.gemstone.gemfire.modules.session.internal.common;

/**
 * Used to define cache properties
 */
public enum CacheProperty {

  ENABLE_DEBUG_LISTENER(Boolean.class),

  ENABLE_GATEWAY_REPLICATION(Boolean.class),

  ENABLE_GATEWAY_DELTA_REPLICATION(Boolean.class),

  ENABLE_LOCAL_CACHE(Boolean.class),

  REGION_NAME(String.class),

  REGION_ATTRIBUTES_ID(String.class),

  STATISTICS_NAME(String.class),

  /**
   * This parameter can take the following values which match the respective
   * attribute container classes
   * <p/>
   * delta_queued     : QueuedDeltaSessionAttributes delta_immediate  :
   * DeltaSessionAttributes immediate        : ImmediateSessionAttributes queued
   * : QueuedSessionAttributes
   */
  SESSION_DELTA_POLICY(String.class),

  /**
   * This parameter can take the following values:
   * <p/>
   * set (default) set_and_get
   */
  REPLICATION_TRIGGER(String.class);

  Class clazz;

  CacheProperty(Class clazz) {
    this.clazz = clazz;
  }

  public Class getClazz() {
    return clazz;
  }
}
