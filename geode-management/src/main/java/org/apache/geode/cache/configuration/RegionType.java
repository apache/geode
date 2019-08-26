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

package org.apache.geode.cache.configuration;

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

  // this is used to represent regions not supported by the management V2 API. For example Gfsh can
  // create regions with "LOCAL*" types
  UNSUPPORTED,

  /**
   * @deprecated use PARTITION and set the redundancy level to 1
   */
  PARTITION_REDUNDANT,
  /**
   * @deprecated use PARTITION_PERSISTENT and set the redundancy level to 1
   */
  PARTITION_REDUNDANT_PERSISTENT,
  /**
   * @deprecated use PARTITION and set the evictionAction to OVERFLOW_TO_DISK
   */
  // PARTITION_OVERFLOW,
  /**
   * @deprecated use PARTITION and set the redundancy level to 1, and set the evictionAction to OVERFLOW_TO_DISK
   */
  // PARTITION_REDUNDANT_OVERFLOW,
  /**
   * @deprecated use PARTITION_PERSISTENT and set the evictionAction to OVERFLOW_TO_DISK
   */
  // PARTITION_PERSISTENT_OVERFLOW,
  /**
   * @deprecated use PARTITION_PERSISTENT and set the redundancy level to 1 and set the evictionAction to OVERFLOW_TO_DISK
   */
  // PARTITION_REDUNDANT_PERSISTENT_OVERFLOW,
  /**
   * @deprecated use PARTITION and set the evictionAction to LOCAL_DESTROY
   */
  // PARTITION_HEAP_LRU,
  /**
   * @deprecated use PARTITION and set the redundancy level to 1 and set the evictionAction to LOCAL_DESTROY
   */
  // PARTITION_REDUNDANT_HEAP_LRU,
  /**
   * @deprecated use PARTITION_PROXY and set the redundancy level to 1
   */
  PARTITION_PROXY_REDUNDANT;
  /**
   * @deprecated use REPLICATE and set the evictionAction to OVERFLOW_TO_DISK
   */
  // REPLICATE_OVERFLOW,
  /**
   * @deprecated use REPLICATE_PERSISTENT and set the evictionAction to OVERFLOW_TO_DISK
   */
  // REPLICATE_PERSISTENT_OVERFLOW,
  /**
   * @deprecated use REPLICATE and set the evictionAction to LOCAL_DESTROY
   */
  // REPLICATE_HEAP_LRU;

  public boolean withProxy() {
    return name().contains("PROXY");
  }

  public boolean withPersistent() {
    return name().contains("PERSISTENT");
  }

  public boolean withReplicate() {
    return name().contains("REPLICATE");
  }

  public boolean withRedundant() {
    return name().contains("REDUNDANT");
  }
}
