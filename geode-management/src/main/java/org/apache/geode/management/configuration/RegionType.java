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

package org.apache.geode.management.configuration;

import org.apache.geode.annotations.Experimental;

/**
 * these are the region types supported by Cluster Management V2 API.
 * these corresponds to a subset of data policies
 */
@Experimental
public enum RegionType {
  PARTITION,
  PARTITION_PERSISTENT,
  PARTITION_PROXY,

  REPLICATE,
  REPLICATE_PERSISTENT,
  REPLICATE_PROXY,

  // this is used to represent regions not supported by the management API. For example Gfsh can
  // create regions with "LOCAL*" types
  LEGACY;

  /**
   * @return if the type contains "PROXY"
   */
  public boolean withProxy() {
    return name().contains("PROXY");
  }

  /**
   * @return if the type contains "PERSISTENT"
   */
  public boolean withPersistent() {
    return name().contains("PERSISTENT");
  }

  /**
   * @return if the type contains "REPLICATE"
   */
  public boolean withReplicate() {
    return name().contains("REPLICATE");
  }

  /**
   * @return if the type contains "PARTITION"
   */
  public boolean withPartition() {
    return name().contains("PARTITION");
  }
}
