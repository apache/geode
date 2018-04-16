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
package org.apache.geode.experimental.driver;

import java.io.IOException;
import java.util.Set;

import org.apache.geode.annotations.Experimental;

/**
 * Defines the behaviors of a driver for communicating with a GemFire server by way of the new
 * protocol.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
public interface Driver {
  /**
   * Retrieves a set of unique names of regions in the GemFire server to which this driver is
   * connected.
   *
   * @return Set of strings of names that uniquely identify regions.
   */
  Set<String> getRegionNames() throws IOException;

  /**
   * Creates an implementation of the region interface for the region with the unique name of
   * <code>regionName</code>.
   *
   * @param regionName String that uniquely identifies the region.
   * @param <K> Type of region keys.
   * @param <V> Type of region values.
   * @return the region object
   */
  <K, V> Region<K, V> getRegion(String regionName);

  /**
   * Creates a new query service or retrieves an extant query service.
   *
   * @return Query service.
   */
  QueryService getQueryService();

  /**
   * Creates a new function service or retrieves an extant function service.
   *
   * @return Function service.
   */
  FunctionService getFunctionService();

  /**
   * Close this Driver, rendering it useless
   */
  void close();

  /**
   * Is this driver connected?
   */
  boolean isConnected();
}
