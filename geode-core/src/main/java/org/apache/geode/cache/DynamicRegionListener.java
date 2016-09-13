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

/**
 * <code>DynamicRegionListener</code> is an interface that can be
 * implemented to handle dynamic region-related events.

 * The methods on a <code>DynamicRegionListener</code> are invoked synchronously.
 * If the listener method takes a long time to execute then it will cause the
 * operation that caused it to be invoked to take a long time.
 * <p>
 * Note: It is possible to receive duplicate create events when the DynamicRegionFactory
 * goes active due to Cache creation.
 * <p>
 * See {@link DynamicRegionFactory}
 *
 * @since GemFire 4.3
 */
public interface DynamicRegionListener {

  /**
   * Handles the 'before region creation' event of a dynamic region. This method
   * is invoked before the dynamic region is created in the local VM.
   *
   * @param parentRegionName The name of the parent region
   * @param regionName The name of the region being created
   */
  public void beforeRegionCreate(String parentRegionName, String regionName);

  /**
   * Handles the 'after region creation' event of a dynamic region. This method
   * is invoked after the dynamic region is created in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void afterRegionCreate(RegionEvent<?,?> event);

  /**
   * Handles the 'before region destroyed' event of a dynamic region. This method
   * is invoked before the dynamic region is destroyed in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void beforeRegionDestroy(RegionEvent<?,?> event);

  /**
   * Handles the 'after region destroyed' event of a dynamic region. This method
   * is invoked after the dynamic region is destroyed in the local VM.
   *
   * @param event A <code>RegionEvent</code> describing the event
   */
  public void afterRegionDestroy(RegionEvent<?,?> event);
}
