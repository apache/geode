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

package com.gemstone.gemfire.cache.client;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.QueryService;

/**
 * A ClientCache instance controls the life cycle of the local singleton cache in a client. 
 * <p>A ClientCache is created using {@link ClientCacheFactory#create}.
 * See {@link ClientCacheFactory} for common usage patterns for creating the client cache instance.
 * <p>ClientCache provides
 * access to functionality when a member connects as a client to GemFire servers.
 * It provides the following services:
 <ul>
 * <li> Access to existing regions (see {@link #getRegion} and {@link #rootRegions}).
 * <li> Creation of regions (see {@link #createClientRegionFactory(ClientRegionShortcut)} and {@link #createClientRegionFactory(String)}).
 * <li> Access the query service (see {@link #getQueryService} and {@link #getLocalQueryService}).</li>
 * <li> Access the GemFire logger (see {@link #getLogger}).</li>
 * <li> Access the GemFire distributed system (see {@link #getDistributedSystem}).</li>
 * <li> Access the GemFire resource manager (see {@link #getResourceManager}).</li>
 * <li> Manages local disk stores for this cache instance (see {@link #createDiskStoreFactory}).</li>
 * <li> Creation of authenticated cache views that support multiple users (see {@link #createAuthenticatedView}).
 </ul>
 * <p>A ClientCache connects to a server using a {@link Pool}. This pool can be
 * configured in the ClientCacheFactory (by default GemFire tries to create a pool
 * which tries to connect to a server on the localhost on port 40404). This default pool
 * is used by {@link Region}s (created using {@link ClientRegionFactory}) to talk to
 * regions on the server.
 * <p>More pools can be created using {@link PoolManager} or by declaring them in cache.xml.
 * @since 6.5
 * @author darrel
 */
public interface ClientCache extends GemFireCache {
  /**
   * Return the QueryService for the named pool.
   * The query operations performed
   * using this QueryService will be executed on the servers that are associated
   * with this pool.
   */
  public QueryService getQueryService(String poolName);
  
  /**
   * Return a QueryService that queries the local state in the client cache.
   * These queries will not be sent to a server.
   */
  public QueryService getLocalQueryService();
  
  /**
   * Terminates this object cache and releases all the resources.
   * Calls {@link Region#close} on each region in the cache.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * {@link CacheClosedException}, unless otherwise noted.
   * @param keepalive whether the server should keep the durable client's queues alive for the timeout period
   * @throws CacheClosedException if the cache is already closed.
   */
  public void close(boolean keepalive);  

  /**
   * Create and return a client region factory that is initialized to create
   * a region using the given predefined region attributes.
   * @param shortcut the predefined region attributes to initialize the factory with.
   * @return a factory that will produce a client region.
   */
  public <K,V> ClientRegionFactory<K,V> createClientRegionFactory(ClientRegionShortcut shortcut);

  /**
   * Create and return a client region factory that is initialized to create
   * a region using the given named region attributes.
   * <p>Named region attributes are defined in cache.xml by setting the name as
   * the value of the <code>id</code> attribute on a <code>region-attributes</code> element.
   * @param regionAttributesId the named region attributes to initialize the factory with.
   * @throws IllegalStateException if named region attributes has not been defined.
   * @return a factory that will produce a client region.
   */
  public <K,V> ClientRegionFactory<K,V> createClientRegionFactory(String regionAttributesId);

  /**
   * Notifies the server that this durable client is ready to receive updates.
   * This method is used by durable clients to notify servers that they
   * are ready to receive updates. As soon as the server receives this message, 
   * it will forward updates to this client (if necessary).
   * <p>
   * Durable clients must call this method after they are done creating regions
   * and issuing interest registration requests.If it is called before then events
   * will be lost.Any time a new {@link Pool} is created and regions have been 
   * added to it then this method needs to be called again.
   * <p>
   *
   * @throws IllegalStateException if called by a non-durable client
   */
  public void readyForEvents();

  /**
   * Creates an authenticated cache view using the given user security properties
   * on the client cache's default pool.
   * Multiple views with different user properties can be created on a
   * single client cache.
   * 
   * Requires that {@link ClientCacheFactory#setPoolMultiuserAuthentication(boolean) multiuser-authentication}
   * to be set to true on the default pool.

   * Applications must use this instance to do operations, when
   * multiuser-authentication is set to true.
   *
   * <p>
   * Authenticated cache views are only allows to access {@link ClientRegionShortcut#PROXY proxy} regions.
   * The {@link RegionService#getRegion} method will throw IllegalStateException
   * if an attempt is made to get a region that has local storage.
   *
   * @param userSecurityProperties
   *          the security properties of a user.
   * @return the {@link RegionService} instance associated with a user and the given
   *         properties.
   * @throws UnsupportedOperationException
   *           when invoked with multiuser-authentication as false.
   */
  public RegionService createAuthenticatedView(Properties userSecurityProperties);
  
  /**
   * Creates an authenticated cache view using the given user security properties
   * using the given pool to connect to servers.
   * Requires that {@link PoolFactory#setMultiuserAuthentication(boolean) multiuser-authentication} to be set to true
   * on the given pool.
   * <p>See {@link #createAuthenticatedView(Properties)} for more information
   * on the returned cache view.
   * @param userSecurityProperties the security properties of a user.
   * @param poolName - the pool that the users should be authenticated against.
   * @return the {@link RegionService} instance associated with a user and the given
   *         properties.
   */
  public RegionService createAuthenticatedView(Properties userSecurityProperties, String poolName);
  
  /**
   * Returns a set of the servers to which this client is currently connected.
   * @since 6.6
   */
  public Set<InetSocketAddress> getCurrentServers();
  
  /**
   * Returns the default server pool. If one or more non-default pools were
   * configured, this may return null.
   * @since 7.0
   * @see com.gemstone.gemfire.cache.client.Pool
   */
  public Pool getDefaultPool();
  
}
