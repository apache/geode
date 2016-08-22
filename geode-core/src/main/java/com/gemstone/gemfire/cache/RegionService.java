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

import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;

/**
 * A RegionService provides access to existing {@link Region regions} that exist
 * in a {@link GemFireCache GemFire cache}.
 * Regions can be obtained using {@link #getRegion}
 * and queried using {@link #getQueryService}.
 * The service should be {@link #close closed} to free up resources
 * once it is no longer needed.
 * Once it {@link #isClosed is closed} any attempt to use it or any {@link Region regions}
 * obtained from it will cause a {@link CacheClosedException} to be thrown.
 * <p>
 * Instances of the interface are created using one of the following methods:
 * <ul>
 * <li> {@link CacheFactory#create()} creates a server instance of {@link Cache}.
 * <li> {@link ClientCacheFactory#create()} creates a client instance of {@link ClientCache}.
 * <li> {@link ClientCache#createAuthenticatedView(Properties)} creates a client multiuser authenticated cache view.
 * </ul>
 * <p>
 *
 * @since GemFire 6.5
 */
public interface RegionService extends AutoCloseable {
  /**
   * the cancellation criterion for this service
   * @return the service's cancellation object
   */
  public CancelCriterion getCancelCriterion();
  /**
   * Return the existing region (or subregion) with the specified
   * path.
   * Whether or not the path starts with a forward slash it is interpreted as a
   * full path starting at a root.
   *
   * @param path the path to the region
   * @return the Region or null if not found
   * @throws IllegalArgumentException if path is null, the empty string, or "/"
   */
  public <K,V> Region<K,V> getRegion(String path);

  /**
   * Returns unmodifiable set of the root regions that are in the region service.
   * This set is a snapshot; it is not backed by the region service.
   *
   * @return a Set of regions
   */
  public Set<Region<?,?>> rootRegions();
  
  // We did not have time to add this feature to 6.6.2
//  /**
//   * Returns a factory that can create a {@link PdxInstance}.
//   * If you want to be able to deserialize the PdxInstance then name
//   * must be a correct class name and expectDomainClass should be set to true.
//   * If you want to just create an object that will always be a PdxInstance set expectDomainClass to false.
//   * @param name the name of the pdx type that
//   * the PdxInstance will represent. If expectDomainClass is true then
//   * this must be the full class and package name of the domain class.
//   * Otherwise it just needs to be a unique string that identifies this instances type.
//   * @param expectDomainClass if true then during deserialization a domain class will
//   * be expected. If false then this type will always deserialize to a PdxInstance
//   * even if read-serialized is false and {@link PdxInstance#getObject()} will return
//   * the PdxInstance.
//   * @return the factory
//   */
//  public PdxInstanceFactory createPdxInstanceFactory(String name, boolean expectDomainClass);

  /**
   * Returns a factory that can create a {@link PdxInstance}.
   * @param className the fully qualified class name that the PdxInstance will become
   *   when it is fully deserialized.
   * @return the factory
   * @since GemFire 6.6.2
   */
  public PdxInstanceFactory createPdxInstanceFactory(String className);
  /**
   * Creates and returns a PdxInstance that represents an enum value.
   * @param className the name of the enum class
   * @param enumName the name of the enum constant
   * @param enumOrdinal the ordinal value of the enum constant
   * @return a PdxInstance that represents the enum value
   * @throws IllegalArgumentException if className or enumName are <code>null</code>.
   * @since GemFire 6.6.2
   */
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal);

  /**
   * Return the QueryService for this region service.
   * For a region service in a client the returned QueryService will
   * execute queries on the server.
   * For a region service not in a client the returned QueryService will
   * execute queries on the local and peer regions.
   */
  public QueryService getQueryService();
  /**
   * Terminates this region service and releases all its resources.
   * Calls {@link Region#close} on each region in the service.
   * After this service is closed, any further
   * method calls on this service or any region object
   * obtained from the service will throw
   * {@link CacheClosedException}, unless otherwise noted.
   * @throws CacheClosedException if the service is already closed.
   */
  public void close();
  /**
   * Indicates if this region service has been closed.
   * After a new service is created, this method returns false;
   * After close is called on this service, this method
   * returns true. This method does not throw <code>CacheClosedException</code>
   * if the service is closed.
   *
   * @return true, if this service has just been created or has started to close; false, otherwise
   */
  public boolean isClosed();
}
