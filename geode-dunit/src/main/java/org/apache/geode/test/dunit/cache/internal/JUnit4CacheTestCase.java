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
package org.apache.geode.test.dunit.cache.internal;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * This class is the base class for all distributed tests using JUnit 4 that require the creation of
 * a {@link Cache}.
 */
public abstract class JUnit4CacheTestCase extends JUnit4DistributedTestCase
    implements CacheTestFixture {

  private static final Logger logger = LogService.getLogger();
  /**
   * The Cache from which regions are obtained.
   *
   * <p>
   * All references synchronized via {@code JUnit4CacheTestCase.class}.
   *
   * <p>
   * Field is static so it doesn't get serialized with SerializableRunnable inner classes.
   */
  protected static InternalCache cache;

  private final CacheTestFixture cacheTestFixture;

  public JUnit4CacheTestCase() {
    this(null);
  }

  JUnit4CacheTestCase(final CacheTestFixture cacheTestFixture) {
    super(cacheTestFixture);
    if (cacheTestFixture == null) {
      this.cacheTestFixture = this;
    } else {
      this.cacheTestFixture = cacheTestFixture;
    }
  }

  /**
   * Creates the {@code Cache} for this test
   */
  private final void createCache() {
    createCache(false);
  }

  private final void createCache(final boolean client) {
    createCache(client, null);
  }

  private final void createCache(final boolean client, final CacheFactory factory) {
    synchronized (JUnit4CacheTestCase.class) {
      try {
        System.setProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        InternalCache newCache;
        if (client) {
          System.setProperty(GEMFIRE_PREFIX + "locators", "");
          System.setProperty(GEMFIRE_PREFIX + MCAST_PORT, "0");
          newCache = (InternalCache) new ClientCacheFactory(getSystem().getProperties()).create();
        } else {
          if (factory == null) {
            newCache = (InternalCache) CacheFactory.create(getSystem());
          } else {
            Properties config = getSystem().getProperties();
            for (Map.Entry entry : config.entrySet()) {
              factory.set((String) entry.getKey(), (String) entry.getValue());
            }
            newCache = (InternalCache) factory.create();
          }
        }
        cache = newCache;
      } finally {
        System.clearProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
        System.clearProperty(GEMFIRE_PREFIX + "locators");
        System.clearProperty(GEMFIRE_PREFIX + MCAST_PORT);
      }
    }
  }

  /**
   * Creates the {@code Cache} for this test that is not connected to other members.
   */
  public final InternalCache createLonerCache() {
    synchronized (JUnit4CacheTestCase.class) {
      try {
        System.setProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        InternalCache newCache = (InternalCache) CacheFactory.create(getLonerSystem());
        cache = newCache;
      } finally {
        System.clearProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }
      return cache;
    }
  }

  /**
   * Sets this test up with a {@code CacheCreation} as its cache. Any existing cache is closed.
   * Whoever calls this must also call {@code finishCacheXml}.
   */
  public static synchronized void beginCacheXml() {
    closeCache();
    cache = new TestCacheCreation();
  }

  /**
   * Finish what {@code beginCacheXml} started. It does this be generating a cache.xml file and then
   * creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final String name) {
    synchronized (JUnit4CacheTestCase.class) {
      try {
        File file = new File(name + "-cache.xml");
        PrintWriter printWriter = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(cache, printWriter);
        printWriter.close();
        cache = null;
        GemFireCacheImpl.testCacheXml = file;
        createCache();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }
  }

  /**
   * Finish what {@code beginCacheXml} started. It does this be generating a cache.xml file and then
   * creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final File root, final String name, final boolean useSchema,
      final String xmlVersion) throws IOException {
    synchronized (JUnit4CacheTestCase.class) {
      File dir = new File(root, "XML_" + xmlVersion);
      dir.mkdirs();
      File file = new File(dir, name + ".xml");
      PrintWriter printWriter = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(cache, printWriter, useSchema, xmlVersion);
      printWriter.close();
      cache = null;
      GemFireCacheImpl.testCacheXml = file;
      try {
        createCache();
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }
  }

  /**
   * Return a cache for obtaining regions, created lazily.
   */
  public final InternalCache getCache() {
    return getCache(false);
  }

  public final InternalCache getCache(final CacheFactory factory) {
    return getCache(false, factory);
  }

  public final InternalCache getCache(final Properties properties) {
    getSystem(properties);
    return getCache();
  }

  public final InternalCache getCache(final boolean client) {
    return getCache(client, null);
  }

  public final InternalCache getCache(final boolean client, final CacheFactory factory) {
    synchronized (JUnit4CacheTestCase.class) {
      InternalCache gemFireCache = GemFireCacheImpl.getInstance();
      if (gemFireCache != null && !gemFireCache.isClosed()
          && gemFireCache.getCancelCriterion().isCancelInProgress()) {
        Awaitility.await("waiting for cache to close").atMost(30, TimeUnit.SECONDS)
            .until(gemFireCache::isClosed);
      }
      if (cache == null || cache.isClosed()) {
        cache = null;
        createCache(client, factory);
      }
      if (client && cache != null) {
        IgnoredException.addIgnoredException("java.net.ConnectException");
      }
      return cache;
    }
  }

  /**
   * Creates a client cache from the factory if one does not already exist.
   *
   * @since GemFire 6.5
   */
  public final ClientCache getClientCache(final ClientCacheFactory factory) {
    synchronized (JUnit4CacheTestCase.class) {
      InternalCache gemFireCache = GemFireCacheImpl.getInstance();
      if (gemFireCache != null && !gemFireCache.isClosed()
          && gemFireCache.getCancelCriterion().isCancelInProgress()) {
        Awaitility.await("waiting for cache to close").atMost(30, TimeUnit.SECONDS)
            .until(gemFireCache::isClosed);
      }
      if (cache == null || cache.isClosed()) {
        cache = null;
        disconnectFromDS();
        cache = (InternalCache) factory.create();
      }
      if (cache != null) {
        IgnoredException.addIgnoredException("java.net.ConnectException");
      }
      return (ClientCache) cache;
    }
  }

  public final ClientCache getClientCache() {
    return (ClientCache) cache;
  }

  /**
   * Invokes {@link #getCache()} and casts the return to {@code GemFireCacheImpl}.
   *
   * <p>
   * TODO: change all callers to use getCache and delete getGemfireCache
   *
   * @deprecated Please use {@link #getCache} which returns InternalCache instead.
   */
  @Deprecated
  public final GemFireCacheImpl getGemfireCache() {
    return (GemFireCacheImpl) getCache();
  }

  public static final synchronized boolean hasCache() {
    return cache != null;
  }

  /**
   * Return current cache without creating one.
   */
  public static final synchronized InternalCache basicGetCache() {
    return cache;
  }

  /**
   * Close the cache.
   */
  public static final synchronized void closeCache() {
    // Workaround for the fact that some classes are now extending
    // CacheTestCase but not using it properly.
    if (cache == null) {
      cache = GemFireCacheImpl.getInstance();
    }
    try {
      if (cache != null) {
        try {
          if (!cache.isClosed()) {
            if (cache instanceof GemFireCacheImpl) {
              // this unnecessary type-cast prevents NoSuchMethodError
              // java.lang.NoSuchMethodError:
              // org.apache.geode.internal.cache.InternalCache.getTxManager()Lorg/apache/geode/internal/cache/TXManagerImpl
              CacheTransactionManager transactionManager =
                  ((GemFireCacheImpl) cache).getTxManager();
              if (transactionManager != null) {
                if (transactionManager.exists()) {
                  try {
                    // make sure we cleanup this threads txid stored in a thread local
                    transactionManager.rollback();
                  } catch (Exception ignore) {

                  }
                }
              }
            }
            cache.close();
          }
        } finally {
          cache = null;
        }
      } // cache != null
    } finally {
      // Make sure all pools are closed, even if we never created a cache
      PoolManager.close(false);
    }
  }

  /**
   * Close the cache in all VMs.
   */
  protected final void closeAllCache() {
    closeCache();
    Invoke.invokeInEveryVM(JUnit4CacheTestCase::closeCache);
  }

  @Override
  public final void preTearDown() throws Exception {
    preTearDownCacheTestCase();
    tearDownCacheTestCase();
    postTearDownCacheTestCase();
  }

  private final void tearDownCacheTestCase() {
    remoteTearDown();
    Invoke.invokeInEveryVM(JUnit4CacheTestCase::remoteTearDown);
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    if (this.cacheTestFixture != this) {
      this.cacheTestFixture.preTearDownCacheTestCase();
    }
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
    if (this.cacheTestFixture != this) {
      this.cacheTestFixture.postTearDownCacheTestCase();
    }
  }

  /**
   * Local destroy all root regions and close the cache.
   */
  private static synchronized void remoteTearDown() {
    try {
      DistributionMessageObserver.setInstance(null);
      destroyRegions(cache);
    } finally {
      try {
        closeCache();
      } finally {
        try {
          cleanDiskDirs();
        } catch (Exception e) {
          logger.error("Error cleaning disk dirs", e);
        }
      }
    }
  }

  /**
   * Returns a region with the given name and attributes.
   */
  public final Region createRegion(final String name, final RegionAttributes attributes)
      throws CacheException {
    return createRegion(name, "root", attributes);
  }

  public final Region createRegion(final String name, final String rootName,
      final RegionAttributes attributes) throws CacheException {
    Region root = getRootRegion(rootName);
    if (root == null) {
      // don't put listeners on root region
      AttributesFactory attributesFactory = new AttributesFactory(attributes);
      ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;

      attributesFactory.setCacheLoader(null);
      attributesFactory.setCacheWriter(null);
      attributesFactory.setPoolName(null);
      attributesFactory.setPartitionAttributes(null);
      attributesFactory.setRegionTimeToLive(expiration);
      attributesFactory.setEntryTimeToLive(expiration);
      attributesFactory.setRegionIdleTimeout(expiration);
      attributesFactory.setEntryIdleTimeout(expiration);

      RegionAttributes rootAttrs = attributesFactory.create();
      root = createRootRegion(rootName, rootAttrs);
    }

    return root.createSubregion(name, attributes);
  }

  public final Region getRootRegion() {
    return getRootRegion("root");
  }

  public final Region getRootRegion(final String rootName) {
    return getCache().getRegion(rootName);
  }

  protected final Region createRootRegion(final RegionAttributes attributes)
      throws RegionExistsException, TimeoutException {
    return createRootRegion("root", attributes);
  }

  public final Region createRootRegion(final String rootName, final RegionAttributes attributes)
      throws RegionExistsException, TimeoutException {
    return getCache().createRegion(rootName, attributes);
  }

  public final Region createExpiryRootRegion(final String rootName,
      final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      return createRootRegion(rootName, attributes);
    } finally {
      System.clearProperty(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  /**
   * @deprecated Please use {@link IgnoredException#addIgnoredException(String)} instead.
   */
  @Deprecated
  public final CacheSerializableRunnable addExceptionTag1(final String exceptionStringToIgnore) {
    return new CacheSerializableRunnable("addExceptionTag") {
      @Override
      public void run2() {
        getCache().getLogger().info(
            "<ExpectedException action=add>" + exceptionStringToIgnore + "</ExpectedException>");
      }
    };
  }

  /**
   * TODO: delete removeExceptionTag1 method
   *
   * @deprecated Please use {@link IgnoredException#addIgnoredException(String)} instead.
   */
  @Deprecated
  public final CacheSerializableRunnable removeExceptionTag1(final String exceptionStringToIgnore) {
    return new CacheSerializableRunnable("removeExceptionTag") {

      @Override
      public void run2() throws CacheException {
        getCache().getLogger().info(
            "<ExpectedException action=remove>" + exceptionStringToIgnore + "</ExpectedException>");
      }
    };
  }

  public static final File getDiskDir() {
    int vmNum = VM.getCurrentVMNum();
    File dir = new File("diskDir", "disk" + String.valueOf(vmNum)).getAbsoluteFile();
    dir.mkdirs();
    return dir;
  }

  /**
   * Return a set of disk directories for persistence tests. These directories will be automatically
   * cleaned up on test case closure.
   */
  public static final File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }

  /**
   * Used to generate a cache.xml. Basically just a {@code CacheCreation} with a few more methods
   * implemented.
   */
  private static class TestCacheCreation extends CacheCreation {
    private boolean closed = false;

    @Override
    public void close() {
      this.closed = true;
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }
  }
}
