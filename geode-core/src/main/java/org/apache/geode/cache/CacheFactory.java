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

import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS;

import java.util.Properties;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SecurityConfig;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

/**
 * Factory class used to create the singleton {@link Cache cache} and connect to the GemFire
 * singleton {@link DistributedSystem distributed system}. If the application wants to connect to
 * GemFire as a client it should use {@link org.apache.geode.cache.client.ClientCacheFactory}
 * instead.
 * <p>
 * Once the factory has been configured using its {@link #set(String, String)} method you produce a
 * {@link Cache} by calling the {@link #create()} method.
 * <p>
 * To get the existing unclosed singleton cache instance call {@link #getAnyInstance}.
 * <p>
 * If an instance of {@link DistributedSystem} already exists when this factory creates a cache,
 * that instance will be used if it is compatible with this factory.
 * <p>
 * The following examples illustrate bootstrapping the cache using region shortcuts:
 * <p>
 * Example 1: Create a cache and a replicate region named customers.
 *
 * <PRE>
 * Cache c = new CacheFactory().create();
 * Region r = c.createRegionFactory(REPLICATE).create("customers");
 * </PRE>
 *
 * Example 2: Create a cache and a partition region with redundancy
 *
 * <PRE>
 * Cache c = new CacheFactory().create();
 * Region r = c.createRegionFactory(PARTITION_REDUNDANT).create("customers");
 * </PRE>
 *
 * Example 3: Construct the cache region declaratively in cache.xml
 *
 * <PRE>
  &lt;!DOCTYPE cache PUBLIC
    "-//GemStone Systems, Inc.//GemFire Declarative Caching 8.0//EN"
    "http://www.gemstone.com/dtd/cache8_0.dtd">
  &lt;cache>
    &lt;region name="myRegion" refid="REPLICATE"/>
      &lt;!-- you can override or add to the REPLICATE attributes by adding
           a region-attributes sub element here -->
  &lt;/cache>
 * </PRE>
 *
 * Now, create the cache telling it to read your cache.xml file:
 *
 * <PRE>
 * Cache c = new CacheFactory().set("cache-xml-file", "myCache.xml").create();
 * Region r = c.getRegion("myRegion");
 * </PRE>
 *
 * <p>
 * For a complete list of all region shortcuts see {@link RegionShortcut}. Applications that need to
 * explicitly control the individual region attributes can do this declaratively in XML or using
 * APIs.
 *
 * @since GemFire 3.0
 */
public class CacheFactory {

  private final Properties dsProps;

  private final CacheConfig cacheConfig = new CacheConfig();

  /**
   * Creates a default cache factory.
   *
   * @since GemFire 6.5
   */
  public CacheFactory() {
    this(null);
  }

  /**
   * Create a CacheFactory initialized with the given gemfire properties. For a list of valid
   * GemFire properties and their meanings see {@linkplain ConfigurationProperties}.
   *
   * @param props the gemfire properties to initialize the factory with.
   * @since GemFire 6.5
   */
  public CacheFactory(Properties props) {
    if (props == null) {
      props = new Properties();
    }
    this.dsProps = props;
  }

  /**
   * Sets a gemfire property that will be used when creating the Cache. For a list of valid GemFire
   * properties and their meanings see {@link ConfigurationProperties}.
   *
   * @param name the name of the gemfire property
   * @param value the value of the gemfire property
   * @return a reference to this CacheFactory object
   * @since GemFire 6.5
   */
  public CacheFactory set(String name, String value) {
    this.dsProps.setProperty(name, value);
    return this;
  }

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
  public static synchronized Cache create(DistributedSystem system) throws CacheExistsException,
      TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    return create(system, false, new CacheConfig());
  }

  private static synchronized Cache create(DistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig) throws CacheExistsException, TimeoutException, CacheWriterException,
      GatewayException, RegionExistsException {
    // Moved code in this method to GemFireCacheImpl.create
    return GemFireCacheImpl.create((InternalDistributedSystem) system, existingOk, cacheConfig);
  }

  /**
   * Creates a new cache that uses the configured distributed system. If a connected distributed
   * system already exists it will be used if it is compatible with the properties on this factory.
   * Otherwise a a distributed system will be created with the configured properties. If a cache
   * already exists it will be returned.
   * <p>
   * If the cache does need to be created it will also be initialized from cache.xml if it exists.
   *
   * @return the created or already existing singleton cache
   *
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   * @throws IllegalStateException if cache already exists and is not compatible with the new
   *         configuration.
   * @throws AuthenticationFailedException if authentication fails.
   * @throws AuthenticationRequiredException if the distributed system is in secure mode and this
   *         new member is not configured with security credentials.
   * @since GemFire 6.5
   */
  public Cache create()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    synchronized (CacheFactory.class) {
      DistributedSystem ds = null;
      if (this.dsProps.isEmpty() && !ALLOW_MULTIPLE_SYSTEMS) {
        // any ds will do
        ds = InternalDistributedSystem.getConnectedInstance();
        validateUsabilityOfSecurityCallbacks(ds);
      }
      if (ds == null) {
        // use ThreadLocal to avoid exposing new User API in DistributedSystem
        SecurityConfig.set(this.cacheConfig.getSecurityManager(),
            this.cacheConfig.getPostProcessor());
        try {
          ds = DistributedSystem.connect(this.dsProps);
        } finally {
          SecurityConfig.remove();
        }
      }
      return create(ds, true, this.cacheConfig);
    }
  }

  /**
   * Throws GemFireSecurityException if existing DistributedSystem connection cannot use specified
   * SecurityManager or PostProcessor.
   */
  private void validateUsabilityOfSecurityCallbacks(DistributedSystem ds)
      throws GemFireSecurityException {
    if (ds == null) {
      return;
    }
    // pre-existing DistributedSystem already has an incompatible SecurityService in use
    if (this.cacheConfig.getSecurityManager() != null) {
      // invalid configuration
      throw new GemFireSecurityException(
          "Existing DistributedSystem connection cannot use specified SecurityManager");
    }
    if (this.cacheConfig.getPostProcessor() != null) {
      // invalid configuration
      throw new GemFireSecurityException(
          "Existing DistributedSystem connection cannot use specified PostProcessor");
    }
  }

  /**
   * Gets the instance of {@link Cache} produced by an earlier call to {@link #create()}.
   *
   * @param system the {@code DistributedSystem} the cache was created with.
   * @return the {@link Cache} associated with the specified system.
   * @throws CacheClosedException if a cache has not been created or the created one is
   *         {@link Cache#isClosed closed}
   */
  public static Cache getInstance(DistributedSystem system) {
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
  public static Cache getInstanceCloseOk(DistributedSystem system) {
    return basicGetInstance(system, true);
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
  public static synchronized Cache getAnyInstance() {
    InternalCache instance = GemFireCacheImpl.getInstance();
    if (instance == null) {
      throw new CacheClosedException(
          "A cache has not yet been created.");
    } else {
      instance.getCancelCriterion().checkCancelInProgress(null);
      return instance;
    }
  }

  /**
   * Returns the version of the cache implementation.
   *
   * @return the version of the cache implementation as a {@code String}
   */
  public static String getVersion() {
    return GemFireVersion.getGemFireVersion();
  }

  /**
   * Sets the object preference to PdxInstance type. When a cached object that was serialized as a
   * PDX is read from the cache a {@link PdxInstance} will be returned instead of the actual domain
   * class. The PdxInstance is an interface that provides run time access to the fields of a PDX
   * without deserializing the entire PDX. The PdxInstance implementation is a light weight wrapper
   * that simply refers to the raw bytes of the PDX that are kept in the cache. Using this method
   * applications can choose to access PdxInstance instead of Java object.
   * <p>
   * Note that a PdxInstance is only returned if a serialized PDX is found in the cache. If the
   * cache contains a deserialized PDX, then a domain class instance is returned instead of a
   * PdxInstance.
   *
   * @param readSerialized true to prefer PdxInstance
   * @return this CacheFactory
   * @since GemFire 6.6
   * @see org.apache.geode.pdx.PdxInstance
   */
  public CacheFactory setPdxReadSerialized(boolean readSerialized) {
    this.cacheConfig.setPdxReadSerialized(readSerialized);
    return this;
  }

  /**
   * Sets the securityManager for the cache. If this securityManager is set, it will override the
   * security-manager property you set in your gemfire system properties.
   *
   * This is provided mostly for container to inject an already initialized securityManager. An
   * object provided this way is expected to be initialized already. We are not calling the init
   * method on this object
   *
   * @return this CacheFactory
   */
  public CacheFactory setSecurityManager(SecurityManager securityManager) {
    this.cacheConfig.setSecurityManager(securityManager);
    return this;
  }

  /**
   * Sets the postProcessor for the cache. If this postProcessor is set, it will override the
   * security-post-processor setting in the gemfire system properties.
   *
   * This is provided mostly for container to inject an already initialized post processor. An
   * object provided this way is expected to be initialized already. We are not calling the init
   * method on this object
   *
   * @return this CacheFactory
   */
  public CacheFactory setPostProcessor(PostProcessor postProcessor) {
    this.cacheConfig.setPostProcessor(postProcessor);
    return this;
  }

  /**
   * Set the PDX serializer for the cache. If this serializer is set, it will be consulted to see if
   * it can serialize any domain classes which are added to the cache in portable data exchange
   * format.
   *
   * @param serializer the serializer to use
   * @return this CacheFactory
   * @since GemFire 6.6
   * @see PdxSerializer
   */
  public CacheFactory setPdxSerializer(PdxSerializer serializer) {
    this.cacheConfig.setPdxSerializer(serializer);
    return this;
  }

  /**
   * Set the disk store that is used for PDX meta data. When serializing objects in the PDX format,
   * the type definitions are persisted to disk. This setting controls which disk store is used for
   * that persistence.
   *
   * If not set, the metadata will go in the default disk store.
   *
   * @param diskStoreName the name of the disk store to use for the PDX metadata.
   * @return this CacheFactory
   * @since GemFire 6.6
   */
  public CacheFactory setPdxDiskStore(String diskStoreName) {
    this.cacheConfig.setPdxDiskStore(diskStoreName);
    return this;
  }

  /**
   * Control whether the type metadata for PDX objects is persisted to disk. The default for this
   * setting is false. If you are using persistent regions with PDX then you must set this to true.
   * If you are using a {@code GatewaySender} or {@code AsyncEventQueue} with PDX then you should
   * set this to true.
   *
   * @param isPersistent true if the metadata should be persistent
   * @return this CacheFactory
   * @since GemFire 6.6
   */
  public CacheFactory setPdxPersistent(boolean isPersistent) {
    this.cacheConfig.setPdxPersistent(isPersistent);
    return this;
  }

  /**
   * Control whether pdx ignores fields that were unread during deserialization. The default is to
   * preserve unread fields be including their data during serialization. But if you configure the
   * cache to ignore unread fields then their data will be lost during serialization.
   * <P>
   * You should only set this attribute to {@code true} if you know this member will only be reading
   * cache data. In this use case you do not need to pay the cost of preserving the unread fields
   * since you will never be reserializing pdx data.
   *
   * @param ignore {@code true} if fields not read during pdx deserialization should be ignored;
   *        {@code false}, the default, if they should be preserved.
   * @return this CacheFactory
   * @since GemFire 6.6
   */
  public CacheFactory setPdxIgnoreUnreadFields(boolean ignore) {
    this.cacheConfig.setPdxIgnoreUnreadFields(ignore);
    return this;
  }
}
