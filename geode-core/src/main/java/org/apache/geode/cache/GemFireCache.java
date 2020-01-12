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

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;

import org.apache.geode.LogWriter;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.pdx.PdxSerializer;

/**
 * GemFireCache represents the singleton cache that must be created in order to use GemFire in a
 * Java virtual machine. Users must create either a {@link Cache} for a peer/server JVM or a
 * {@link ClientCache} for a client JVM. Instances of this interface are created using one of the
 * following methods:
 * <ul>
 * <li>{@link CacheFactory#create()} creates a peer/server instance of {@link Cache}.
 * <li>{@link ClientCacheFactory#create()} creates a client instance of {@link ClientCache}.
 * </ul>
 *
 * @since GemFire 6.5
 */
public interface GemFireCache extends RegionService {
  /**
   * Returns the name of this cache. This method does not throw {@code CacheClosedException} if the
   * cache is closed.
   *
   * @return the String name of this cache
   */
  String getName();

  /**
   * Returns the distributed system used by this cache. This method does not throw
   * {@code CacheClosedException} if the cache is closed.
   */
  DistributedSystem getDistributedSystem();

  /**
   * Returns the {@code ResourceManager} for managing this cache's resources.
   *
   * @return {@code ResourceManager} for managing this cache's resources
   * @since GemFire 6.0
   */
  ResourceManager getResourceManager();

  /**
   * Sets the "copy on read" feature for cache read operations.
   *
   * @since GemFire 4.0
   */
  void setCopyOnRead(boolean copyOnRead);

  /**
   * Indicates whether the "copy on read" is enabled for this cache.
   *
   * @return true if "copy on read" is enabled, false otherwise.
   *
   * @since GemFire 4.0
   */
  boolean getCopyOnRead();

  /**
   * Returns the {@code RegionAttributes} with the given {@code id} or {@code null} if no
   * {@code RegionAttributes} with that id exists.
   *
   * @see #setRegionAttributes
   *
   * @since GemFire 4.1
   */
  <K, V> RegionAttributes<K, V> getRegionAttributes(String id);

  /**
   * Sets the {@code id} of the given {@code RegionAttributes}. If a region attributes named
   * {@code name} already exists, the mapping will be overwritten with {@code attrs}. However,
   * changing the mapping will not effect existing regions.
   *
   * @param id The id of the region attributes
   * @param attrs The attributes to associate with {@code id}. If {@code attrs} is {@code null}, any
   *        existing {@code RegionAttributes} associated with {@code id} will be removed.
   *
   * @see #getRegionAttributes
   *
   * @since GemFire 4.1
   */
  <K, V> void setRegionAttributes(String id, RegionAttributes<K, V> attrs);

  /**
   * Returns an unmodifiable mapping of ids to region attributes. The keys of the map are
   * {@link String}s and the values of the map are {@link RegionAttributes}.
   *
   * @since GemFire 4.1
   */
  <K, V> Map<String, RegionAttributes<K, V>> listRegionAttributes();

  /**
   * Loads the cache configuration described in a
   * <a href="package-summary.html#declarative">declarative caching XML file</a> into this cache. If
   * the XML describes a region that already exists, any mutable region attributes, indexes, and
   * region entries that are defined in the XML are updated/added.
   *
   * <P>
   *
   * Because this method may perform a {@link Region#put(Object, Object) put} on a {@code Region},
   * it declares that it throws a {@code TimeoutException}, {@code CacheWriterException},
   * {@code GatewayException}, or {@code RegionExistsException}.
   *
   * @throws CacheXmlException If the XML read from {@code is} does not conform to the dtd or if an
   *         {@code IOException} occurs while reading the XML.
   *
   * @since GemFire 4.1
   */
  void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException;

  /**
   * Gets the logging object for GemFire. This method does not throw {@code CacheClosedException} if
   * the cache is closed.
   *
   * @return the logging object
   */
  LogWriter getLogger();

  /**
   * Gets the security logging object for GemFire. This method does not throw
   * {@code CacheClosedException} if the cache is closed.
   *
   * @return the security logging object
   */
  LogWriter getSecurityLogger();

  /**
   * Returns the DiskStore by name or {@code null} if no disk store is found.
   *
   * @param name the name of the disk store to find. If {@code null} then the default disk store, if
   *        it exists, is returned.
   * @since GemFire 6.5
   */
  DiskStore findDiskStore(String name);

  /**
   * create diskstore factory
   *
   * @since GemFire 6.5
   */
  DiskStoreFactory createDiskStoreFactory();

  GatewaySenderFactory createGatewaySenderFactory();

  /**
   * Returns whether { @link PdxInstance} is preferred for PDX types instead of Java object.
   *
   * @see CacheFactory#setPdxReadSerialized(boolean)
   * @see ClientCacheFactory#setPdxReadSerialized(boolean)
   *
   * @since GemFire 6.6
   */
  boolean getPdxReadSerialized();

  /**
   * Returns the PdxSerializer used by this cache, or null if no PDX serializer is defined.
   *
   * @since GemFire 6.6
   * @see CacheFactory#setPdxSerializer(PdxSerializer)
   * @see ClientCacheFactory#setPdxSerializer(PdxSerializer)
   */
  PdxSerializer getPdxSerializer();

  /**
   * Returns the disk store used for PDX meta data
   *
   * @since GemFire 6.6
   * @see CacheFactory#setPdxDiskStore(String)
   * @see ClientCacheFactory#setPdxDiskStore(String)
   */
  String getPdxDiskStore();

  /**
   * Returns true if the PDX metadata for this cache is persistent
   *
   * @since GemFire 6.6
   * @see CacheFactory#setPdxPersistent(boolean)
   * @see ClientCacheFactory#setPdxPersistent(boolean)
   */
  boolean getPdxPersistent();

  /**
   * Returns true if fields that are not read during PDX deserialization should be ignored during
   * the PDX serialization.
   *
   * @since GemFire 6.6
   * @see CacheFactory#setPdxIgnoreUnreadFields(boolean)
   * @see ClientCacheFactory#setPdxIgnoreUnreadFields(boolean)
   */
  boolean getPdxIgnoreUnreadFields();

  /**
   * Registers PDX meta-data given an instance of a domain class that will be serialized
   * with PDX.
   *
   * <p>
   * Note that this method serializes the given instance so PDX must already be configured
   * to do the serialization.
   *
   * <p>
   * Note that if the instance is not of a class that will be serialized with PDX
   * then no meta-data is registered.
   *
   * <p>
   * Note that in most cases this method never needs to be called. Currently it is only
   * needed by the JdbcLoader when gets are done for JDBC data that was not written to the
   * table using geode.
   *
   * @param instance the instance of the domain class for which meta-data is to be registered
   * @throws SerializationException if the instance can not be serialized or is not serialized with
   *         PDX
   * @since Geode 1.6
   */
  void registerPdxMetaData(Object instance);

  /**
   * Get the CacheTransactionManager instance for this Cache.
   *
   * @return The CacheTransactionManager instance.
   *
   * @throws CacheClosedException if the cache is closed.
   *
   * @since GemFire 4.0
   */
  CacheTransactionManager getCacheTransactionManager();

  /**
   * Returns the JNDI context associated with the Cache.
   *
   * @return javax.naming.Context Added as part of providing JTA implementation in Gemfire.
   *
   * @since GemFire 4.0
   */
  Context getJNDIContext();

  /**
   * Returns the Declarable used to initialize this cache or {@code null} if it does not have an
   * initializer.
   *
   * @since GemFire 6.6
   */
  Declarable getInitializer();

  /**
   * Returns the Properties used to initialize the cache initializer or {@code null} if no
   * initializer properties exist.
   *
   * @since GemFire 6.6
   */
  Properties getInitializerProps();
}
