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
package org.apache.geode.cache.client.internal;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CachePerfStats;

public interface InternalClientCache extends ClientCache {

  /**
   * @return true if cache is created using a ClientCacheFactory
   */
  boolean isClient();

  /**
   * Determine whether the specified pool factory matches the pool factory used by this cache.
   *
   * @param poolFactory Prospective pool factory.
   * @throws IllegalStateException When the specified pool factory does not match.
   */
  void validatePoolFactory(PoolFactory poolFactory);

  <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException;

  InternalDistributedSystem getInternalDistributedSystem();

  CachePerfStats getCachePerfStats();

  MeterRegistry getMeterRegistry();

  ClientMetadataService getClientMetadataService();
}
