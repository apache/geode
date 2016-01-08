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

package com.gemstone.gemfire.cache;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.pdx.PdxSerializer;

/**
 * GemFireCache represents the singleton cache that must be created
 * in order to use GemFire in a Java virtual machine.
 * Users must create either a {@link Cache} for a peer/server JVM
 * or a {@link ClientCache} for a client JVM.
 * Instances of this interface are created using one of the following methods:
 * <ul>
 * <li> {@link CacheFactory#create()} creates a peer/server instance of {@link Cache}.
 * <li> {@link ClientCacheFactory#create()} creates a client instance of {@link ClientCache}.
 * </ul>
 *
 * @since 6.5
 * @author darrel
 */
public interface GemFireCache extends RegionService {
  /** Returns the name of this cache.
   * This method does not throw
   * <code>CacheClosedException</code> if the cache is closed.
   * @return the String name of this cache
   */
  public String getName();
  /**
   * Returns the distributed system used by this cache.
   * This method does not throw
   * <code>CacheClosedException</code> if the cache is closed.
   */
  public DistributedSystem getDistributedSystem();
  /**
   * Returns the <code>ResourceManager</code> for managing this cache's 
   * resources.
   * 
   * @return <code>ResourceManager</code> for managing this cache's resources
   * @since 6.0
   */
  public ResourceManager getResourceManager();

  /**
   * Sets the "copy on read" feature for cache read operations.
   *
   * @since 4.0
   */
  public void setCopyOnRead(boolean copyOnRead);

  /**
   * Indicates whether the "copy on read" is enabled for this cache.
   * 
   * @return true if "copy on read" is enabled, false otherwise.
   *
   * @since 4.0
   */
  public boolean getCopyOnRead();

  /**
   * Returns the <code>RegionAttributes</code> with the given
   * <code>id</code> or <code>null</code> if no
   * <code>RegionAttributes</code> with that id exists.
   *
   * @see #setRegionAttributes
   *
   * @since 4.1
   */
  public <K,V> RegionAttributes<K,V> getRegionAttributes(String id);

  /**
   * Sets the <code>id</code> of the given
   * <code>RegionAttributes</code>.  If a region attributes named
   * <code>name</code> already exists, the mapping will be overwritten
   * with <code>attrs</code>.  However, changing the mapping will not
   * effect existing regions.
   *
   * @param id
   *        The id of the region attributes
   * @param attrs
   *        The attributes to associate with <code>id</code>.  If
   *        <code>attrs</code> is <code>null</code>, any existing
   *        <code>RegionAttributes</code> associated with
   *        <code>id</code> will be removed.
   *
   * @see #getRegionAttributes
   *
   * @since 4.1
   */
  public <K,V> void setRegionAttributes(String id, RegionAttributes<K,V> attrs);

  /**
   * Returns an unmodifiable mapping of ids to region attributes.  The
   * keys of the map are {@link String}s and the values of the map are
   * {@link RegionAttributes}.
   *
   * @since 4.1
   */
  public <K,V> Map<String, RegionAttributes<K,V>> listRegionAttributes();

  /**
   * Loads the cache configuration described in a <a
   * href="package-summary.html#declarative">declarative caching XML
   * file</a> into this cache.  If the XML describes a region that
   * already exists, any mutable region attributes, indexes, and
   * region entries that are defined in the XML are updated/added.
   *
   * <P>
   *
   * Because this method may perform a {@link Region#put(Object, Object) put} on a
   * <code>Region</code>, it declares that it throws a
   * <code>TimeoutException</code>, <code>CacheWriterException</code>,
   * <code>GatewayException</code>,
   * or <code>RegionExistsException</code>.
   *
   * @throws CacheXmlException
   *         If the XML read from <code>is</code> does not conform to
   *         the dtd or if an <code>IOException</code> occurs while
   *         reading the XML.
   *
   * @since 4.1
   */
  public void loadCacheXml(InputStream is) 
    throws TimeoutException, CacheWriterException,
           GatewayException,
           RegionExistsException;

  /**
   * Gets the logging object for GemFire.
   * This method does not throw
   * <code>CacheClosedException</code> if the cache is closed.
   * @return the logging object
   */
  public LogWriter getLogger();

  /**
   * Gets the security logging object for GemFire.
   * This method does not throw
   * <code>CacheClosedException</code> if the cache is closed.
   * @return the security logging object
   */
  public LogWriter getSecurityLogger();

  /**
   * Returns the DiskStore by name or <code>null</code> if no disk store is found.
   * @param name the name of the disk store to find. If <code>null</code> then the
   * default disk store, if it exists, is returned.
   * @since 6.5
   */
  public DiskStore findDiskStore(String name);
  
  /**
   * create diskstore factory 
   * 
   * @since 6.5
   */
  public DiskStoreFactory createDiskStoreFactory();
  
  public GatewaySenderFactory createGatewaySenderFactory();
  
  /**
   * Returns whether { @link PdxInstance} is preferred for PDX types instead of Java object.
   * @see com.gemstone.gemfire.cache.CacheFactory#setPdxReadSerialized(boolean)
   * @see com.gemstone.gemfire.cache.client.ClientCacheFactory#setPdxReadSerialized(boolean)
   * 
   * @since 6.6
   */
  public boolean getPdxReadSerialized();
  
  /**
   * Returns the PdxSerializer used by this cache, or null
   * if no PDX serializer is defined.
   * 
   * @since 6.6
   * @see CacheFactory#setPdxSerializer(PdxSerializer)
   * @see ClientCacheFactory#setPdxSerializer(PdxSerializer)
   */
  public PdxSerializer getPdxSerializer();
  
  /**
   * Returns the disk store used for PDX meta data
   * @since 6.6
   * @see CacheFactory#setPdxDiskStore(String)
   * @see ClientCacheFactory#setPdxDiskStore(String)
   */
  public String getPdxDiskStore();
  
  /**
   * Returns true if the PDX metadata for this
   * cache is persistent
   * @since 6.6
   * @see CacheFactory#setPdxPersistent(boolean)
   * @see ClientCacheFactory#setPdxPersistent(boolean)
   */
  public boolean getPdxPersistent();
  /**
   * Returns true if fields that are not read during PDX deserialization
   * should be ignored during the PDX serialization.
   * @since 6.6
   * @see CacheFactory#setPdxIgnoreUnreadFields(boolean)
   * @see ClientCacheFactory#setPdxIgnoreUnreadFields(boolean)
   */
  public boolean getPdxIgnoreUnreadFields();
  /**
   * Get the CacheTransactionManager instance for this Cache.
   *
   * @return The CacheTransactionManager instance.
   *
   * @throws CacheClosedException if the cache is closed.
   *
   * @since 4.0
   */
  public CacheTransactionManager getCacheTransactionManager();
  /**
   * Returns the JNDI context associated with the Cache.
   * @return javax.naming.Context
   * Added as part of providing JTA implementation in Gemfire.
   *
   * @since 4.0
   */
  public Context getJNDIContext();

  /**
   * Returns the Declarable used to initialize this cache or <code>null</code>
   * if it does not have an initializer.
   * @since 6.6
   */
  public Declarable getInitializer();

  /**
   * Returns the Properties used to initialize the cache initializer or
   * <code>null</code> if no initializer properties exist.
   * @since 6.6
   */
  public Properties getInitializerProps();
}
