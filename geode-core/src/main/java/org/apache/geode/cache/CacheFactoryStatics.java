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
package org.apache.geode.cache;

import java.util.Properties;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheFactory;
import org.apache.geode.internal.cache.LocalRegion;

class CacheFactoryStatics {

  /**
   * Creates a new cache that uses the specified {@code system}.
   * <p>
   * The {@code system} can specify a
   * <A href="../distributed/DistributedSystem.html#cache-xml-file">"cache-xml-file"</a> property
   * which will cause this creation to also create the regions, objects, and attributes declared in
   * the file. The contents of the file must comply with the {@code "doc-files/cache8_0.dtd">} file.
   * Note that when parsing the XML file {@link Declarable} classes are loaded using the current
   * thread's {@linkplain Thread#getContextClassLoader context class loader}.
   *
   * @param system a {@code DistributedSystem} obtained by calling
   *        {@link DistributedSystem#connect}.
   *
   * @return a {@code Cache} that uses the specified {@code system} for distribution.
   *
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
   * @deprecated as of 6.5 use {@link #CacheFactory(Properties)} instead.
   */
  @Deprecated
  static Cache create(DistributedSystem system) throws CacheExistsException,
      TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    return new InternalCacheFactory()
        .setIsExistingOk(false)
        .create((InternalDistributedSystem) system);
  }

  /**
   * Gets the instance of {@link Cache} produced by an earlier call to {@link #create()}.
   *
   * @param system the {@code DistributedSystem} the cache was created with.
   * @return the {@link Cache} associated with the specified system.
   * @throws CacheClosedException if a cache has not been created or the created one is
   *         {@link Cache#isClosed closed}
   */
  static Cache getInstance(DistributedSystem system) {
    return basicGetInstance(system, false);
  }

  /**
   * Gets the instance of {@link Cache} produced by an earlier call to {@link #create()} even if it
   * has been closed.
   *
   * @param system the {@code DistributedSystem} the cache was created with.
   * @return the {@link Cache} associated with the specified system.
   * @throws CacheClosedException if a cache has not been created
   * @since GemFire 3.5
   */
  static Cache getInstanceCloseOk(DistributedSystem system) {
    return basicGetInstance(system, true);
  }

  /**
   * Gets an arbitrary open instance of {@link Cache} produced by an earlier call to
   * {@link #create()}.
   *
   * <p>
   * WARNING: To avoid risk of deadlock, do not invoke getAnyInstance() from within any
   * CacheCallback including CacheListener, CacheLoader, CacheWriter, TransactionListener,
   * TransactionWriter. Instead use EntryEvent.getRegion().getCache(),
   * RegionEvent.getRegion().getCache(), LoaderHelper.getRegion().getCache(), or
   * TransactionEvent.getCache().
   * </p>
   *
   * @throws CacheClosedException if a cache has not been created or the only created one is
   *         {@link Cache#isClosed closed}
   */
  static Cache getAnyInstance() {
    synchronized (InternalCacheFactory.class) {
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

  /**
   * Returns the version of the cache implementation.
   *
   * @return the version of the cache implementation as a {@code String}
   */
  static String getVersion() {
    return GemFireVersion.getGemFireVersion();
  }

  private static Cache basicGetInstance(DistributedSystem system, boolean closeOk) {
    // Avoid synchronization if this is an initialization thread to avoid
    // deadlock when messaging returns to this VM
    final int initReq = LocalRegion.threadInitLevelRequirement();
    if (initReq == LocalRegion.ANY_INIT || initReq == LocalRegion.BEFORE_INITIAL_IMAGE) { // fix bug
      // 33471
      return basicGetInstancePart2(system, closeOk);
    } else {
      synchronized (CacheFactory.class) {
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
