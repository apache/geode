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
import java.util.Set;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

/**
 * A RegionService provides access to existing {@link Region regions} that exist in a
 * {@link GemFireCache GemFire cache}. Regions can be obtained using {@link #getRegion} and queried
 * using {@link #getQueryService}. The service should be {@link #close closed} to free up resources
 * once it is no longer needed. Once it {@link #isClosed is closed} any attempt to use it or any
 * {@link Region regions} obtained from it will cause a {@link CacheClosedException} to be thrown.
 *
 * <p>
 * Instances of the interface are created using one of the following methods:
 * <ul>
 * <li>{@link CacheFactory#create()} creates a server instance of {@link Cache}.
 * <li>{@link ClientCacheFactory#create()} creates a client instance of {@link ClientCache}.
 * <li>{@link ClientCache#createAuthenticatedView(Properties)} creates a client multiuser
 * authenticated cache view.
 * </ul>
 *
 * @since GemFire 6.5
 */
public interface RegionService extends AutoCloseable {
  /**
   * the cancellation criterion for this service
   *
   * @return the service's cancellation object
   */
  CancelCriterion getCancelCriterion();

  /**
   * Return the existing region (or subregion) with the specified path. Whether or not the path
   * starts with a forward slash it is interpreted as a full path starting at a root.
   *
   * @param path the path to the region
   * @return the Region or null if not found
   * @throws IllegalArgumentException if path is null, the empty string, or "/"
   */
  <K, V> Region<K, V> getRegion(String path);

  /**
   * Returns unmodifiable set of the root regions that are in the region service. This set is a
   * snapshot; it is not backed by the region service.
   *
   * @return a Set of regions
   */
  Set<Region<?, ?>> rootRegions();

  /**
   * Returns a factory that can create a {@link PdxInstance}.
   *
   * @param className the fully qualified class name that the PdxInstance will become when it is
   *        fully deserialized unless {@link PdxInstanceFactory#neverDeserialize()} is called.
   *        If className is the empty string then no class versioning will be done for that
   *        PdxInstance and {@link PdxInstanceFactory#neverDeserialize()} will be automatically
   *        called on the returned factory.
   * @return the factory
   * @throws IllegalArgumentException if className is {@code null}.
   * @since GemFire 6.6.2
   */
  PdxInstanceFactory createPdxInstanceFactory(String className);

  /**
   * Creates and returns a PdxInstance that represents an enum value.
   *
   * @param className the name of the enum class
   * @param enumName the name of the enum constant
   * @param enumOrdinal the ordinal value of the enum constant
   * @return a PdxInstance that represents the enum value
   * @throws IllegalArgumentException if className or enumName are {@code null}.
   * @since GemFire 6.6.2
   */
  PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal);

  /**
   * Return the QueryService for this region service. For a region service in a client the returned
   * QueryService will execute queries on the server. For a region service not in a client the
   * returned QueryService will execute queries on the local and peer regions.
   */
  QueryService getQueryService();


  /**
   * Returns the JSONFormatter. In the multi-user case, this will return a JSONFormatter
   * that has the knowledge of the user associated with this region service.
   *
   */
  JSONFormatter getJsonFormatter();

  /**
   * Terminates this region service and releases all its resources. Calls {@link Region#close} on
   * each region in the service. After this service is closed, any further method calls on this
   * service or any region object obtained from the service will throw {@link CacheClosedException},
   * unless otherwise noted.
   *
   * @throws CacheClosedException if the service is already closed.
   */
  @Override
  void close();

  /**
   * Indicates if this region service has been closed. After a new service is created, this method
   * returns false; After close is called on this service, this method returns true. This method
   * does not throw {@code CacheClosedException} if the service is closed.
   *
   * @return true, if this service has just been created or has started to close; false, otherwise
   */
  boolean isClosed();
}
