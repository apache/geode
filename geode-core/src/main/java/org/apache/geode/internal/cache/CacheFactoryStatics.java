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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;

/**
 * Implementation of static methods in {@link CacheFactory} including factory create methods and
 * singleton getters.
 */
public class CacheFactoryStatics {

  /**
   * @throws IllegalArgumentException If {@code system} is not {@link DistributedSystem#isConnected
   *         connected}.
   * @throws CacheExistsException If an open cache already exists.
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} instead.
   */
  @Deprecated
  public static Cache create(DistributedSystem system) throws CacheExistsException,
      TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    return new InternalCacheBuilder()
        .setIsExistingOk(false)
        .create((InternalDistributedSystem) system);
  }

  /**
   * @throws CacheClosedException if a cache has not been created or the created one is
   *         {@link Cache#isClosed closed}
   */
  public static Cache getInstance(DistributedSystem system) {
    return basicGetInstance(system, false);
  }

  /**
   * @throws CacheClosedException if a cache has not been created
   */
  public static Cache getInstanceCloseOk(DistributedSystem system) {
    return basicGetInstance(system, true);
  }

  /**
   * @throws CacheClosedException if a cache has not been created or the only created one is
   *         {@link Cache#isClosed closed}
   */
  public static InternalCache getAnyInstance() {
    synchronized (InternalCacheBuilder.class) {
      InternalCache instance = GemFireCacheImpl.getInstance();
      if (instance == null) {
        throw new CacheClosedException(
            "A cache has not yet been created.");
      } else {
        instance.getCancelCriterion().checkCancelInProgress(null);
        return instance;
      }
    }
  }

  private static Cache basicGetInstance(DistributedSystem system, boolean closeOk) {
    // Avoid synchronization if this is an initialization thread to avoid
    // deadlock when messaging returns to this VM
    final InitializationLevel initReq = LocalRegion.getThreadInitLevelRequirement();
    if (initReq == ANY_INIT || initReq == BEFORE_INITIAL_IMAGE) {
      return basicGetInstancePart2(system, closeOk);
    } else {
      synchronized (InternalCacheBuilder.class) {
        return basicGetInstancePart2(system, closeOk);
      }
    }
  }

  private static Cache basicGetInstancePart2(DistributedSystem system, boolean closeOk) {
    InternalCache instance = GemFireCacheImpl.getInstance();
    if (instance == null) {
      throw new CacheClosedException(
          "A cache has not yet been created.");
    } else {
      if (instance.isClosed() && !closeOk) {
        throw instance.getCacheClosedException(
            "The cache has been closed.", null);
      }
      if (!instance.getDistributedSystem().equals(system)) {
        throw instance.getCacheClosedException(
            "A cache has not yet been created for the given distributed system.");
      }
      return instance;
    }
  }
}
