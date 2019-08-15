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
 * these are the region types supported by Cluster Management V2 API. The attributes of these
 * region types are the same as their namesakes in RegionShortcut
 */
@Experimental
public enum RegionType {
  PARTITION,
  PARTITION_REDUNDANT,
  PARTITION_PERSISTENT,
  PARTITION_REDUNDANT_PERSISTENT,
  PARTITION_OVERFLOW,
  PARTITION_REDUNDANT_OVERFLOW,
  PARTITION_PERSISTENT_OVERFLOW,
  PARTITION_REDUNDANT_PERSISTENT_OVERFLOW,
  PARTITION_HEAP_LRU,
  PARTITION_REDUNDANT_HEAP_LRU,

  PARTITION_PROXY,
  PARTITION_PROXY_REDUNDANT,

  REPLICATE,
  REPLICATE_PERSISTENT,
  REPLICATE_OVERFLOW,
  REPLICATE_PERSISTENT_OVERFLOW,
  REPLICATE_HEAP_LRU,

  REPLICATE_PROXY,

  // this is used to represent regions not supported by the management V2 API. For example Gfsh can
  // create regions with "LOCAL*" types
  UNSUPPORTED
}
