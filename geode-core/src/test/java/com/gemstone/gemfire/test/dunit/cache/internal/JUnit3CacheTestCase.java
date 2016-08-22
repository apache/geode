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
import java.io.IOException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.internal.JUnit3DistributedTestCase;

/**
 * The abstract superclass of tests that require the creation of a
 * {@link Cache}.
 */
public abstract class JUnit3CacheTestCase extends JUnit3DistributedTestCase implements CacheTestFixture {

  private final JUnit4CacheTestCase delegate = new JUnit4CacheTestCase(this){};

  public JUnit3CacheTestCase(final String name) {
    super(name);
  }

  /**
   * Creates the {@code Cache} for this test that is not connected to other
   * members.
   */
  public final Cache createLonerCache() {
    return delegate.createLonerCache();
  }

  /**
   * Sets this test up with a {@code CacheCreation} as its cache. Any existing
   * cache is closed. Whoever calls this must also call {@code finishCacheXml}.
   */
  public static final void beginCacheXml() {
    JUnit4CacheTestCase.beginCacheXml();
  }

  /**
   * Finish what {@code beginCacheXml} started. It does this be generating a
   * cache.xml file and then creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final String name) {
    delegate.finishCacheXml(name);
  }

  /**
   * Finish what {@code beginCacheXml} started. It does this be generating a
   * cache.xml file and then creating a real cache using that cache.xml.
   */
  public final void finishCacheXml(final String name, final boolean useSchema, final String xmlVersion) {
    delegate.finishCacheXml(name, useSchema, xmlVersion);
  }

  /**
   * Return a cache for obtaining regions, created lazily.
   */
  public final Cache getCache() {
    return delegate.getCache();
  }

  public final Cache getCache(final CacheFactory factory) {
    return delegate.getCache(factory);
  }

  public final Cache getCache(final boolean client) {
    return delegate.getCache(client);
  }

  public final Cache getCache(final boolean client, final CacheFactory factory) {
    return delegate.getCache(client, factory);
  }

  /**
   * Creates a client cache from the factory if one does not already exist.
   *
   * @since GemFire 6.5
   */
  public final ClientCache getClientCache(final ClientCacheFactory factory) {
    return delegate.getClientCache(factory);
  }

  /**
   * Invokes {@link #getCache()} and casts the return to
   * {@code GemFireCacheImpl}.
   */
  public final GemFireCacheImpl getGemfireCache() { // TODO: remove?
    return delegate.getGemfireCache();
  }

  public static final boolean hasCache() {
    return JUnit4CacheTestCase.hasCache();
  }

  /**
   * Return current cache without creating one.
   */
  public static final Cache basicGetCache() {
    return JUnit4CacheTestCase.basicGetCache();
  }

  /**
   * Close the cache.
   */
  public static final void closeCache() {
    JUnit4CacheTestCase.closeCache();
  }

  /**
   * Closed the cache in all VMs.
   */
  protected final void closeAllCache() {
    delegate.closeAllCache();
  }

  @Override
  public final void preTearDown() throws Exception {
    delegate.preTearDown();
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
  }

  @Override
  public void postTearDownCacheTestCase() throws Exception {
  }

  /**
   * Local destroy all root regions and close the cache.
   */
  protected static final void remoteTearDown() {
    JUnit4CacheTestCase.remoteTearDown();
  }

  /**
   * Returns a region with the given name and attributes
   */
  public final Region createRegion(final String name, final RegionAttributes attributes) throws CacheException {
    return delegate.createRegion(name, attributes);
  }

  public final Region createRegion(final String name, final String rootName, final RegionAttributes attributes) throws CacheException {
    return delegate.createRegion(name, rootName, attributes);
  }

  public final Region getRootRegion() {
    return delegate.getRootRegion();
  }

  public final Region getRootRegion(final String rootName) {
    return delegate.getRootRegion(rootName);
  }

  protected final Region createRootRegion(final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    return delegate.createRootRegion(attributes);
  }

  public final Region createRootRegion(final String rootName, final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    return delegate.createRootRegion(rootName, attributes);
  }

  public final Region createExpiryRootRegion(final String rootName, final RegionAttributes attributes) throws RegionExistsException, TimeoutException {
    return delegate.createExpiryRootRegion(rootName, attributes);
  }

  /**
   * @deprecated Please use {@link IgnoredException#addIgnoredException(String)} instead.
   */
  @Deprecated
  public final CacheSerializableRunnable addExceptionTag1(final String exceptionStringToIgnore) {
    return delegate.addExceptionTag1(exceptionStringToIgnore);
  }

  /**
   * @deprecated Please use {@link IgnoredException#remove()} instead.
   */
  @Deprecated
  public final CacheSerializableRunnable removeExceptionTag1(final String exceptionStringToIgnore) {
    return delegate.removeExceptionTag1(exceptionStringToIgnore);
  }

  public static final File getDiskDir() {
    return JUnit4CacheTestCase.getDiskDir();
  }

  /**
   * Return a set of disk directories for persistence tests. These directories
   * will be automatically cleaned up during tear down.
   */
  public static final File[] getDiskDirs() {
    return JUnit4CacheTestCase.getDiskDirs();
  }

  public static final void cleanDiskDirs() throws IOException {
    JUnit4CacheTestCase.cleanDiskDirs();
  }
}
