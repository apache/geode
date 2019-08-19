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

import org.apache.geode.cache.configuration.RegionType;

/**
 * these are the region shortcuts supported by Cluster Management V2 API. The attributes of these
 * region shortcuts are the same as their namesakes in RegionShortcut
 */
public enum RegionShortcut {
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

  REPLICATE_PROXY;

  /**
   * @return the corresponding region type of each of these region shortcut
   */
  public RegionType getRegionType() {
    if (withPartition()) {
      if (withPersistent()) {
        return RegionType.PARTITION_PERSISTENT;
      }
      if (withProxy()) {
        return RegionType.PARTITION_PROXY;
      }
      return RegionType.PARTITION;
    }

    if (withReplicate()) {
      if (withPersistent()) {
        return RegionType.REPLICATE_PERSISTENT;
      }
      if (withProxy()) {
        return RegionType.REPLICATE_PROXY;
      }
      return RegionType.REPLICATE;
    }

    throw new IllegalStateException("Can't reach here.");
  }

  public boolean withPartition() {
    return name().contains("PARTITION");
  }

  public boolean withReplicate() {
    return name().contains("REPLICATE");
  }

  public boolean withPersistent() {
    return name().contains("PERSISTENT");
  }

  public boolean withProxy() {
    return name().contains("PROXY");
  }

  public boolean withRedundant() {
    return name().contains("REDUNDANT");
  }

  public boolean withOverflow() {
    return name().contains("OVERFLOW");
  }

  public boolean withHeapLRU() {
    return name().contains("HEAP_LRU");
  }
}
