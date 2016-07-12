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
package com.gemstone.gemfire.test.dunit.cache.internal;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;

/**
 * This class is the base class for all distributed tests using JUnit 4 that
 * require the creation of a {@link Cache}.
 *
 * TODO: make this class abstract when JUnit3CacheTestCase is deleted
 */
public abstract class JUnit4CacheTestCase extends JUnit4DistributedTestCase implements CacheTestFixture {

  private static final Logger logger = LogService.getLogger();

  /**
   * The Cache from which regions are obtained.
   *
   * <p>All references synchronized via {@code JUnit4CacheTestCase.class}.
   *
   * <p>Field is static so it doesn't get serialized with SerializableRunnable inner classes.
   */
  private static Cache cache;

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
    synchronized(JUnit4CacheTestCase.class) {
      try {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        Cache newCache;
        if (client) {
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "locators", "");
          System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, "0");
          newCache = (Cache)new ClientCacheFactory(getSystem().getProperties()).create();
        } else {
          if(factory == null) {
            newCache = CacheFactory.create(getSystem());
          } else {
            Properties props = getSystem().getProperties();
            for(Map.Entry entry : props.entrySet()) {
              factory.set((String) entry.getKey(), (String)entry.getValue());
            }
            newCache = factory.create();
          }
        }
        cache = newCache;
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e); // TODO: remove error handling

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
        System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "locators");
        System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT);
      }
    }
  }

  /**
   * Creates the {@code Cache} for this test that is not connected to other
   * members.
   */
  public final Cache createLonerCache() {
    synchronized(JUnit4CacheTestCase.class) {
      try {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        Cache newCache = CacheFactory.create(getLonerSystem());
        cache = newCache;
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e); // TODO: remove error handling

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }
      return cache;
    }
  }

  /**
   * Sets this test up with a {@code CacheCreation} as its cache. Any existing
   * cache is closed. Whoever calls this must also call {@code finishCacheXml}.
   */
  public static final synchronized void beginCacheXml() {
    closeCache();
    cache = new TestCacheCreation();
  }

  /**
   * Finish what {@code beginCacheXml} started. It does this be generating a
   * cache.xml file and then creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final String name) {
    synchronized(JUnit4CacheTestCase.class) {
      File file = new File(name + "-cache.xml");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(cache, pw);
        pw.close();
      } catch (IOException ex) {
        Assert.fail("IOException during cache.xml generation to " + file, ex); // TODO: remove error handling
      }
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
   * Finish what {@code beginCacheXml} started. It does this be generating a
   * cache.xml file and then creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final String name, final boolean useSchema, final String xmlVersion) {
    synchronized(JUnit4CacheTestCase.class) {
      File dir = new File("XML_" + xmlVersion);
      dir.mkdirs();
      File file = new File(dir, name + ".xml");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(cache, pw, useSchema, xmlVersion);
        pw.close();
      } catch (IOException ex) {
        Assert.fail("IOException during cache.xml generation to " + file, ex); // TODO: remove error handling
      }
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
  public final Cache getCache() {
    return getCache(false);
  }

  public final Cache getCache(final CacheFactory factory) {
    return getCache(false, factory);
  }

  public final Cache getCache(final boolean client) {
    return getCache(client, null);
  }

  public final Cache getCache(final boolean client, final CacheFactory factory) {
    synchronized (JUnit4CacheTestCase.class) {
      final GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
      if (gemFireCache != null && !gemFireCache.isClosed()
              && gemFireCache.getCancelCriterion().isCancelInProgress()) {
        Wait.waitForCriterion(new WaitCriterion() { // TODO: replace with Awaitility
          @Override
          public boolean done() {
            return gemFireCache.isClosed();
          }
          @Override
          public String description() {
            return "waiting for cache to close";
          }
        }, 30 * 1000, 300, true);
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
      final GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
      if (gemFireCache != null && !gemFireCache.isClosed()
              && gemFireCache.getCancelCriterion().isCancelInProgress()) {
        Wait.waitForCriterion(new WaitCriterion() { // TODO: replace with Awaitility
          @Override
          public boolean done() {
            return gemFireCache.isClosed();
          }
          @Override
          public String description() {
            return "waiting for cache to close";
          }
        }, 30 * 1000, 300, true);
      }
      if (cache == null || cache.isClosed()) {
        cache = null;
        disconnectFromDS();
        cache = (Cache)factory.create();
      }
      if (cache != null) {
        IgnoredException.addIgnoredException("java.net.ConnectException");
      }
      return (ClientCache)cache;
    }
  }

  /**
   * Invokes {@link #getCache()} and casts the return to
   * {@code GemFireCacheImpl}.
   */
  public final GemFireCacheImpl getGemfireCache() { // TODO: remove?
    return (GemFireCacheImpl)getCache();
  }

  public static final synchronized boolean hasCache() {
    return cache != null;
  }

  /**
   * Return current cache without creating one.
   */
  public static final synchronized Cache basicGetCache() {
    return cache;
  }

  /**
   * Close the cache.
   */
  public static final synchronized void closeCache() {
    // Workaround for that fact that some classes are now extending
    // CacheTestCase but not using it properly.
    if(cache == null) {
      cache = GemFireCacheImpl.getInstance();
    }
    try {
      if (cache != null) {
        try {
          if (!cache.isClosed()) {
            if (cache instanceof GemFireCacheImpl) {
              CacheTransactionManager txMgr = ((GemFireCacheImpl)cache).getTxManager();
              if (txMgr != null) {
                if (txMgr.exists()) {
                  try {
                    // make sure we cleanup this threads txid stored in a thread local
                    txMgr.rollback();
                  }catch(Exception ignore) {

                  }
                }
              }
            }
            cache.close();
          }
        }
        finally {
          cache = null;
        }
      } // cache != null
    } finally {
      //Make sure all pools are closed, even if we never
      //created a cache
      PoolManager.close(false);
    }
  }

  /**
   * Close the cache in all VMs.
   */
  protected final void closeAllCache() {
    closeCache();
    Invoke.invokeInEveryVM(()->closeCache());
  }

  @Override
  public final void preTearDown() throws Exception {
    preTearDownCacheTestCase();
    tearDownCacheTestCase();
    postTearDownCacheTestCase();
  }

  private final void tearDownCacheTestCase() {
    remoteTearDown();
    Invoke.invokeInEveryVM(()->remoteTearDown());
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
  protected static final synchronized void remoteTearDown() {
    try {
      DistributionMessageObserver.setInstance(null);
      destroyRegions(cache);
    }
    finally {
      try {
        closeCache();
      }
      finally {
        try {
          cleanDiskDirs();
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Error cleaning disk dirs", e);
        }
      }
    }
  }

  /**
   * Returns a region with the given name and attributes.
   */
  public final Region createRegion(final String name, final RegionAttributes attributes) throws CacheException {
    return createRegion(name, "root", attributes);
  }

  /**
   * Provide any internal region arguments, typically required when internal
   * use (aka meta-data) regions are needed.
   *
   * @return internal arguments, which may be null.  If null, then default
   *         InternalRegionArguments are used to construct the Region
   */
  private final InternalRegionArguments getInternalRegionArguments() { // TODO: delete?
    return null;
  }

  public final Region createRegion(final String name, final String rootName, final RegionAttributes attributes) throws CacheException {
    Region root = getRootRegion(rootName);
    if (root == null) {
      // don't put listeners on root region
      RegionAttributes rootAttrs = attributes;
      AttributesFactory fac = new AttributesFactory(attributes);
      ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;

      // fac.setCacheListener(null);
      fac.setCacheLoader(null);
      fac.setCacheWriter(null);
      fac.setPoolName(null);
      fac.setPartitionAttributes(null);
      fac.setRegionTimeToLive(expiration);
      fac.setEntryTimeToLive(expiration);
      fac.setRegionIdleTimeout(expiration);
      fac.setEntryIdleTimeout(expiration);
      rootAttrs = fac.create();
      root = createRootRegion(rootName, rootAttrs);
    }

    InternalRegionArguments internalArgs = getInternalRegionArguments();
    if (internalArgs == null) {
      return root.createSubregion(name, attributes);
    } else {
      try {
        LocalRegion lr = (LocalRegion) root;
        return lr.createSubregion(name, attributes, internalArgs);
      } catch (IOException ioe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(ioe);
        throw assErr;
      } catch (ClassNotFoundException cnfe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(cnfe);
        throw assErr;
      }
    }
  }

  public final Region getRootRegion() {
    return getRootRegion("root");
  }

  public final Region getRootRegion(final String rootName) {
    return getCache().getRegion(rootName);
  }

  protected final Region createRootRegion(final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    return createRootRegion("root", attributes);
  }

  public final Region createRootRegion(final String rootName, final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    return getCache().createRegion(rootName, attributes);
  }

  public final Region createExpiryRootRegion(final String rootName, final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
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
  public final CacheSerializableRunnable addExceptionTag1(final String exceptionStringToIgnore) { // TODO: delete this method
    CacheSerializableRunnable addExceptionTag = new CacheSerializableRunnable("addExceptionTag") {
      @Override
      public void run2() {
        getCache().getLogger().info("<ExpectedException action=add>" + exceptionStringToIgnore + "</ExpectedException>");
      }
    };
    return addExceptionTag;
  }

  /**
   * @deprecated Please use {@link IgnoredException#addIgnoredException(String)} instead.
   */
  @Deprecated
  public final CacheSerializableRunnable removeExceptionTag1(final String exceptionStringToIgnore) { // TODO: delete this method
    CacheSerializableRunnable removeExceptionTag = new CacheSerializableRunnable("removeExceptionTag") {
      @Override
      public void run2() throws CacheException {
        getCache().getLogger().info("<ExpectedException action=remove>" + exceptionStringToIgnore + "</ExpectedException>");
      }
    };
    return removeExceptionTag;
  }

  public static final File getDiskDir() {
    int vmNum = VM.getCurrentVMNum();
    File dir = new File("diskDir", "disk" + String.valueOf(vmNum)).getAbsoluteFile();
    dir.mkdirs();
    return dir;
  }

  /**
   * Return a set of disk directories for persistence tests. These directories
   * will be automatically cleaned up on test case closure.
   */
  public static final File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }

  public static final void cleanDiskDirs() throws IOException {
    FileUtil.delete(getDiskDir());
    Arrays.stream(new File(".").listFiles()).forEach(file -> deleteBACKUPDiskStoreFile(file));
  }

  private static void deleteBACKUPDiskStoreFile(final File file) {
    if(file.getName().startsWith("BACKUPDiskStore-")){
      try {
        FileUtil.delete(file);
      }
      catch (IOException e) {
        throw new RuntimeException("Unable to delete BACKUPDiskStore file", e);
      }
    }
  }

  /**
   * Used to generate a cache.xml. Basically just a {@code CacheCreation} with
   * a few more methods implemented.
   */
  private static final class TestCacheCreation extends CacheCreation {
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
