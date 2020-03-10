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

package org.apache.geode.cache.client;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.metrics.internal.InternalDistributedSystemMetricsService;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.net.SSLParameterExtension;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;

/**
 * Factory class used to create the singleton {@link ClientCache client cache} and connect to one or
 * more GemFire Cache Servers. If the application wants to connect to GemFire as a peer it should
 * use {@link org.apache.geode.cache.CacheFactory} instead.
 * <p>
 * Once the factory has been configured using its set* methods you produce a {@link ClientCache} by
 * calling the {@link #create} method. The
 * {@link org.apache.geode.distributed.ConfigurationProperties#CACHE_XML_FILE} property can be used
 * to specify a cache.xml file to initialize the cache with. The contents of this file must comply
 * with the <code>"doc-files/cache8_0.dtd"</code> file and the top level element must be a
 * <code>client-cache</code> element.
 * <p>
 * Client connections are managed through connection {@link Pool pools}. ClientCacheFactory creates
 * a single pool to use by default on the cache it creates. ClientCacheFactory can also be used to
 * configure the default connection pool using its <code>setPool*</code> and <code>addPool*</code>
 * methods. In most cases, the defaults used by this implementation will suffice. For the default
 * pool attributes see {@link PoolFactory}. If no pool is configured and a pool was not declared in
 * cache.xml or created using {@link PoolManager} then a default one will be created that connects
 * to a server on the default cache server port and local host. If multiple pools are declared in
 * cache.xml or created by the PoolFactory then no default pool will exist and
 * <code>ClientRegionFactory.setPoolName</code> will need to be called on each region created.
 * <p>
 * To get the existing unclosed singleton client cache instance call {@link #getAnyInstance}.
 * <p>
 * The following examples illustrate bootstrapping the client cache using region shortcuts:
 * <p>
 * Example 1: Connect to a CacheServer on the default host and port and access a region "customers"
 *
 * <PRE>
 * ClientCache c = new ClientCacheFactory().create();
 * Region r = c.createClientRegionFactory(PROXY).create("customers");
 * // The PROXY shortcut tells GemFire to route all requests to the servers
 * // . i.e. there is no local caching
 * </PRE>
 *
 * Example 2: Connect using the GemFire locator and create a local LRU cache
 *
 * <PRE>
 * ClientCache c = new ClientCacheFactory().addPoolLocator(host, port).create();
 * Region r = c.createClientRegionFactory(CACHING_PROXY_HEAP_LRU).create("customers");
 * // The local LRU "customers" data region will automatically start evicting, by default, at 80%
 * // heap utilization threshold
 * </PRE>
 *
 * Example 3: Access the query service
 *
 * <PRE>
 * QueryService qs = new ClientCacheFactory().create().getQueryService();
 * </PRE>
 *
 * Example 4: Construct the client cache region declaratively in cache.xml
 *
 * <PRE>
 * &lt;!DOCTYPE client-cache PUBLIC
 * "-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN"
 * "http://www.gemstone.com/dtd/cache8_0.dtd">
 * &lt;client-cache>
 * &lt;pool name="myPool">
 * &lt;locator host="hostName" port="10334"/>
 * &lt;/pool>
 * &lt;region name="myRegion" refid="PROXY"/>
 * &lt;!-- you can override or add to the PROXY attributes by adding
 * a region-attributes sub element here -->
 * &lt;/client-cache>
 * </PRE>
 *
 * Now, create the cache telling it to read your cache.xml file:
 *
 * <PRE>
 * ClientCache c = new ClientCacheFactory().set("cache-xml-file", "myCache.xml").create();
 * Region r = c.getRegion("myRegion");
 * </PRE>
 * <p>
 * <p>
 * For a complete list of all client region shortcuts see {@link ClientRegionShortcut}. Applications
 * that need to explicitly control the individual region attributes can do this declaratively in XML
 * or using API.
 * <p>
 * Example 5: Define custom region attributes for persistence in XML and create region using API.
 * Define new region attributes with ID "MYAPP_CACHING_PROXY_MEM_LRU" that overrides the
 * "CACHING_PROXY" shortcut
 *
 * <PRE>
 * &lt;!DOCTYPE client-cache PUBLIC
 * "-//GemStone Systems, Inc.//GemFire Declarative Caching 8.0//EN"
 * "http://www.gemstone.com/dtd/cache8_0.dtd">
 * &lt;client-cache>
 * &lt;!-- now create a named region attributes that uses the CACHING_PROXY shortcut
 * and adds a memory LRU limited to 900 megabytes -->
 * &lt;region-attributes id="MYAPP_CACHING_PROXY_MEM_LRU" refid="CACHING_PROXY" >
 * &lt;lru-memory-size maximum="900"/>
 * &lt;/region-attributes>
 * &lt;/client-cache>
 * </PRE>
 *
 * Now, create the data region in the client cache using this new attributes ID.
 *
 * <PRE>
 * ClientCache c = new ClientCacheFactory().set("cache-xml-file", "myCache.xml")
 *     .addPoolLocator(host, port).create();
 * Region r = c.createClientRegionFactory("MYAPP_CACHING_PROXY_MEM_LRU").create("customers");
 * </PRE>
 *
 * @since 6.5
 */
public class ClientCacheFactory {

  private PoolFactory pf;

  private final Properties dsProps;

  private final CacheConfig cacheConfig = new CacheConfig();

  /**
   * Creates a new client cache factory.
   */
  public ClientCacheFactory() {
    dsProps = new Properties();
  }

  /**
   * Create a new client cache factory given the initial gemfire properties.
   *
   * @param props The initial gemfire properties to be used. These properties can be overridden
   *        using the {@link #set} method For a full list of valid gemfire properties see
   *        {@link org.apache.geode.distributed.ConfigurationProperties}.
   */
  public ClientCacheFactory(Properties props) {
    if (props == null) {
      props = new Properties();
    }
    dsProps = props;
  }

  /**
   * Sets a gemfire property that will be used when creating the ClientCache. For a full list of
   * valid gemfire properties see {@link org.apache.geode.distributed.ConfigurationProperties}.
   *
   * @param name the name of the gemfire property
   * @param value the value of the gemfire property
   * @return a reference to this ClientCacheFactory object
   */
  public ClientCacheFactory set(String name, String value) {
    dsProps.setProperty(name, value);
    return this;
  }

  /**
   * Create a singleton client cache. If a client cache already exists in this vm that is not
   * compatible with this factory's configuration then create will fail.
   * <p>
   * While creating the cache instance any declarative cache configuration (cache.xml) is processed
   * and used to initialize the created cache.
   * <P>
   * Note that the cache that is produced is a singleton. Before a different instance can be
   * produced the old one must be {@link ClientCache#close closed}.
   *
   * @return the singleton client cache
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a <code>CacheWriterException</code> is thrown while
   *         initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   * @throws IllegalStateException if a client cache already exists and it is not compatible with
   *         this factory's configuration.
   * @throws IllegalStateException if mcast-port or locator is set on client cache.
   * @throws AuthenticationFailedException if authentication fails.
   * @throws AuthenticationRequiredException if server is in secure mode and client cache is not
   *         configured with security credentials.
   */
  public ClientCache create() {
    return basicCreate();
  }

  @SuppressWarnings("deprecation")
  private static InternalClientCache getInternalClientCache() {
    return GemFireCacheImpl.getInstance();
  }

  private ClientCache basicCreate() {
    synchronized (ClientCacheFactory.class) {
      InternalClientCache instance = getInternalClientCache();

      {
        String propValue = dsProps.getProperty(MCAST_PORT);
        if (propValue != null) {
          int mcastPort = Integer.parseInt(propValue);
          if (mcastPort != 0) {
            throw new IllegalStateException(
                "On a client cache the mcast-port must be set to 0 or not set. It was set to "
                    + mcastPort);
          }
        }
      }
      {
        String propValue = dsProps.getProperty(LOCATORS);
        if (propValue != null && !propValue.isEmpty()) {
          throw new IllegalStateException(
              "On a client cache the locators property must be set to an empty string or not set."
                  + " It was set to \""
                  + propValue + "\".");
        }
      }
      dsProps.setProperty(MCAST_PORT, "0");
      dsProps.setProperty(LOCATORS, "");
      InternalDistributedSystem system = connectInternalDistributedSystem();

      if (instance != null && !instance.isClosed()) {
        // this is ok; just make sure it is a client cache
        if (!instance.isClient()) {
          throw new IllegalStateException(
              "A client cache can not be created because a non-client cache already exists.");
        }

        // check if pool is compatible
        instance.validatePoolFactory(pf);

        // Check if cache configuration matches.
        cacheConfig.validateCacheConfig(instance);

        return instance;
      } else {

        return (InternalClientCache) new InternalCacheBuilder(cacheConfig)
            .setIsClient(true)
            .setPoolFactory(pf)
            .create(system);
      }
    }
  }

  private InternalDistributedSystem connectInternalDistributedSystem() {
    MetricsService.Builder metricsServiceBuilder =
        new InternalDistributedSystemMetricsService.Builder()
            .setIsClient(true);
    return InternalDistributedSystem.connectInternal(dsProps, null, metricsServiceBuilder);
  }

  private PoolFactory getPoolFactory() {
    if (pf == null) {
      pf = PoolManager.createFactory();
    }
    return pf;
  }

  /**
   * Sets the socket connect timeout for this pool. The number of milli seconds specified as socket
   * timeout when the client connects to the servers/locators. A timeout of zero is interpreted as
   * an infinite timeout. The connection will then block until established or an error occurs.
   *
   * @param socketConnectTimeout timeout in milliseconds when the client connects to the servers
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>socketConnectTimeout</code> is less than or equal to
   *         <code>-1</code>.
   */
  public ClientCacheFactory setPoolSocketConnectTimeout(int socketConnectTimeout) {
    getPoolFactory().setSocketConnectTimeout(socketConnectTimeout);
    return this;
  }

  /**
   * Sets the free connection timeout for this pool. If the pool has a max connections setting,
   * operations will block if all of the connections are in use. The free connection timeout
   * specifies how long those operations will block waiting for a free connection before receiving
   * an {@link AllConnectionsInUseException}. If max connections is not set this setting has no
   * effect.
   *
   * @param connectionTimeout the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionTimeout</code> is less than or equal to
   *         <code>0</code>.
   * @see #setPoolMaxConnections(int)
   */
  public ClientCacheFactory setPoolFreeConnectionTimeout(int connectionTimeout) {
    getPoolFactory().setFreeConnectionTimeout(connectionTimeout);
    return this;
  }

  /**
   * Sets the server connection timeout for this pool. If the pool has a max connections setting,
   * operations will block if there is no free connections toward designated server. The server
   * connection timeout
   * specifies how long those operations will block waiting for a connection toward server before
   * receiving an {@link AllConnectionsInUseException}. If max connections is not set this setting
   * has no effect.
   *
   * @param connectionTimeout the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionTimeout</code> is less than
   *         <code>0</code>.
   * @see #setPoolMaxConnections(int)
   */
  public ClientCacheFactory setPoolServerConnectionTimeout(int connectionTimeout) {
    getPoolFactory().setServerConnectionTimeout(connectionTimeout);
    return this;
  }

  /**
   * Sets the load conditioning interval for this pool. This interval controls how frequently the
   * pool will check to see if a connection to a given server should be moved to a different server
   * to improve the load balance.
   * <p>
   * A value of <code>-1</code> disables load conditioning
   *
   * @param loadConditioningInterval the connection lifetime in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionLifetime</code> is less than
   *         <code>-1</code>.
   */
  public ClientCacheFactory setPoolLoadConditioningInterval(int loadConditioningInterval) {
    getPoolFactory().setLoadConditioningInterval(loadConditioningInterval);
    return this;
  }

  /**
   * Sets the socket buffer size for each connection made in this pool. Large messages can be
   * received and sent faster when this buffer is larger. Larger buffers also optimize the rate at
   * which servers can send events for client subscriptions.
   *
   * @param bufferSize the size of the socket buffers used for reading and writing on each
   *        connection in this pool.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>bufferSize</code> is less than or equal to
   *         <code>0</code>.
   */
  public ClientCacheFactory setPoolSocketBufferSize(int bufferSize) {
    getPoolFactory().setSocketBufferSize(bufferSize);
    return this;
  }

  /**
   * Sets the thread local connections policy for this pool. If <code>true</code> then any time a
   * thread goes to use a connection from this pool it will check a thread local cache and see if it
   * already has a connection in it. If so it will use it. If not it will get one from this pool and
   * cache it in the thread local. This gets rid of thread contention for the connections but
   * increases the number of connections the servers see.
   * <p>
   * If <code>false</code> then connections are returned to the pool as soon as the operation being
   * done with the connection completes. This allows connections to be shared amonst multiple
   * threads keeping the number of connections down.
   *
   * @param threadLocalConnections if <code>true</code> then enable thread local connections.
   * @return a reference to <code>this</code>
   * @deprecated Since Geode 1.10.0. Thread local connections are ignored. Will be removed in future
   *             major release.
   */
  @Deprecated
  public ClientCacheFactory setPoolThreadLocalConnections(boolean threadLocalConnections) {
    getPoolFactory().setThreadLocalConnections(threadLocalConnections);
    return this;
  }

  /**
   * Sets the number of milliseconds to wait for a response from a server before timing out the
   * operation and trying another server (if any are available).
   *
   * @param timeout number of milliseconds to wait for a response from a server
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>timeout</code> is less than <code>0</code>.
   */
  public ClientCacheFactory setPoolReadTimeout(int timeout) {
    getPoolFactory().setReadTimeout(timeout);
    return this;
  }

  /**
   * Set the minimum number of connections to keep available at all times. When the pool is created,
   * it will create this many connections. If <code>0</code> then connections will not be made until
   * an actual operation is done that requires client-to-server communication.
   *
   * @param minConnections the initial number of connections this pool will create.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>minConnections</code> is less than <code>0</code>.
   */
  public ClientCacheFactory setPoolMinConnections(int minConnections) {
    getPoolFactory().setMinConnections(minConnections);
    return this;
  }

  /**
   * Set the max number of client to server connections that the pool will create. If all of the
   * connections are in use, an operation requiring a client to server connection will block until a
   * connection is available.
   *
   * @param maxConnections the maximum number of connections in the pool. this pool will create. -1
   *        indicates that there is no maximum number of connections
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>maxConnections</code> is less than
   *         <code>minConnections</code>.
   * @see #setPoolFreeConnectionTimeout(int)
   * @see #setPoolServerConnectionTimeout(int)
   */
  public ClientCacheFactory setPoolMaxConnections(int maxConnections) {
    getPoolFactory().setMaxConnections(maxConnections);
    return this;
  }

  /**
   * Set the amount of time a connection can be idle before expiring the connection. If the pool
   * size is greater than the minimum specified by {@link #setPoolMinConnections(int)}, connections
   * which have been idle for longer than the idleTimeout will be closed.
   *
   * @param idleTimeout The amount of time in milliseconds that an idle connection should live
   *        before expiring. -1 indicates that connections should never expire.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code> is less than <code>-1</code>.
   */
  public ClientCacheFactory setPoolIdleTimeout(long idleTimeout) {
    getPoolFactory().setIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * Set the number of times to retry a request after timeout/exception.
   *
   * @param retryAttempts The number of times to retry a request after timeout/exception. -1
   *        indicates that a request should be tried against every available server before failing
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code> is less than <code>-1</code>.
   */
  public ClientCacheFactory setPoolRetryAttempts(int retryAttempts) {
    getPoolFactory().setRetryAttempts(retryAttempts);
    return this;
  }

  /**
   * How often to ping servers to verify that they are still alive. Each server will be sent a ping
   * every pingInterval if there has not been any other communication with the server.
   * <p>
   * These pings are used by the server to monitor the health of the client. Make sure that the
   * pingInterval is less than the maximum time between pings allowed by the cache server.
   *
   * @param pingInterval The amount of time in milliseconds between pings.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>pingInterval</code> is less than or equal to
   *         <code>0</code>.
   * @see CacheServer#setMaximumTimeBetweenPings(int)
   */
  public ClientCacheFactory setPoolPingInterval(long pingInterval) {
    getPoolFactory().setPingInterval(pingInterval);
    return this;
  }

  /**
   * How often to send client statistics to the server. Doing this allows <code>gfmon</code> to
   * monitor clients.
   * <p>
   * A value of <code>-1</code> disables the sending of client statistics to the server.
   *
   * @param statisticInterval The amount of time in milliseconds between sends of client statistics
   *        to the server.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>statisticInterval</code> is less than
   *         <code>-1</code>.
   */
  public ClientCacheFactory setPoolStatisticInterval(int statisticInterval) {
    getPoolFactory().setStatisticInterval(statisticInterval);
    return this;
  }

  /**
   * Configures the group that all servers this pool connects to must belong to.
   *
   * @param group the server group that this pool will connect to. If <code>null</code> or
   *        <code>""</code> then all servers will be connected to.
   * @return a reference to <code>this</code>
   */
  public ClientCacheFactory setPoolServerGroup(String group) {
    getPoolFactory().setServerGroup(group);
    return this;
  }

  /**
   * Add a locator, given its host and port, to this factory. The locator must be a server locator
   * and will be used to discover other running cache servers and locators. Note that if the host is
   * unknown at the time of this call the locator will still be added. When the pool is used for an
   * operation if the host is still unknown an exception will be thrown.
   *
   * @param host the host name or ip address that the locator is listening on.
   * @param port the port that the locator is listening on
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if port is outside the valid range of [0..65535] inclusive.
   * @throws IllegalStateException if a server has already been {@link #addPoolServer added} to this
   *         factory.
   */
  public ClientCacheFactory addPoolLocator(String host, int port) {
    getPoolFactory().addLocator(host, port);
    return this;
  }

  /**
   * Add a server, given its host and port, to this factory. The server must be a cache server and
   * this client will directly connect to without consulting a server locator. Note that if the host
   * is unknown at the time of this call the server will still be added. When the pool is used for
   * an operation if the host is still unknown an exception will be thrown.
   *
   * @param host the host name or ip address that the server is listening on.
   * @param port the port that the server is listening on
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if port is outside the valid range of [0..65535] inclusive.
   * @throws IllegalStateException if a locator has already been {@link #addPoolLocator added} to
   *         this factory.
   */
  public ClientCacheFactory addPoolServer(String host, int port) {
    getPoolFactory().addServer(host, port);
    return this;
  }

  /**
   * If set to <code>true</code> then the created pool will have server-to-client subscriptions
   * enabled. If set to <code>false</code> then all <code>Subscription*</code> attributes are
   * ignored at create time.
   *
   * @return a reference to <code>this</code>
   */
  public ClientCacheFactory setPoolSubscriptionEnabled(boolean enabled) {
    getPoolFactory().setSubscriptionEnabled(enabled);
    return this;
  }

  /**
   * Sets the redundancy level for this pools server-to-client subscriptions. If <code>0</code> then
   * no redundant copies will be kept on the servers. Otherwise an effort will be made to maintain
   * the requested number of copies of the server-to-client subscriptions. At most one copy per
   * server will be made up to the requested level.
   *
   * @param redundancy the number of redundant servers for this client's subscriptions.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>redundancyLevel</code> is less than <code>-1</code>.
   */
  public ClientCacheFactory setPoolSubscriptionRedundancy(int redundancy) {
    getPoolFactory().setSubscriptionRedundancy(redundancy);
    return this;
  }

  /**
   * A server has an inactivity monitor that ensures a message is sent to a client at least once a
   * minute (60,000 milliseconds). If a subscription timeout multipler is set in the client it
   * enables timing out of the subscription feed with failover to another server.
   * <p>
   * The client will time out it's subscription connection after a number of seconds equal to this
   * multiplier times the server's subscription-timeout.
   * <p>
   * Set this to 2 or more to make sure the client will receive pings from the server before the
   * timeout.
   * <p>
   * A value of zero (the default) disables timeouts
   * <p>
   * The resulting timeout will be multiplied by 1.25 in order to avoid race conditions with the
   * server sending its "ping" message.
   */
  public ClientCacheFactory setPoolSubscriptionTimeoutMultiplier(int multiplier) {
    getPoolFactory().setSubscriptionTimeoutMultiplier(multiplier);
    return this;
  }

  /**
   * Sets the messageTrackingTimeout attribute which is the time-to-live period, in milliseconds,
   * for subscription events the client has received from the server. It's used to minimize
   * duplicate events. Entries that have not been modified for this amount of time are expired from
   * the list
   *
   * @param messageTrackingTimeout number of milliseconds to set the timeout to.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>messageTrackingTimeout</code> is less than or equal
   *         to <code>0</code>.
   */
  public ClientCacheFactory setPoolSubscriptionMessageTrackingTimeout(int messageTrackingTimeout) {
    getPoolFactory().setSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
    return this;
  }

  /**
   * Set the socket factory used by this pool to create connections to both locators (if
   * configured using {@link #addPoolLocator(String, int)} (String, int)}) and servers.
   *
   * Sockets returned by this factory will have the rest of the configuration options
   * specified on this pool and on the {@link ClientCache} applied to them. In particular,
   * sockets returned by this factory will be wrapped with SSLSockets if ssl is enabled
   * for this client cache.
   *
   * This factory can be used for configuring a proxy, or overriding various socket settings.
   * For modifying SSL settings, see {@link SSLParameterExtension}
   *
   * See {@link ProxySocketFactories}
   *
   * @param socketFactory The {@link SocketFactory} to use
   * @return a reference to <code> this </code>
   * @see PoolFactory#setSocketFactory(SocketFactory)
   * @since Geode 1.13
   */
  public ClientCacheFactory setPoolSocketFactory(SocketFactory socketFactory) {
    getPoolFactory().setSocketFactory(socketFactory);
    return this;
  }

  /**
   * Sets the interval in milliseconds to wait before sending acknowledgements to the cache server
   * for events received from the server subscriptions.
   *
   * @param ackInterval number of milliseconds to wait before sending event acknowledgements.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>ackInterval</code> is less than or equal to
   *         <code>0</code>.
   */
  public ClientCacheFactory setPoolSubscriptionAckInterval(int ackInterval) {
    getPoolFactory().setSubscriptionAckInterval(ackInterval);
    return this;
  }

  /**
   * By default setPRSingleHopEnabled is <code>true</code> in which case the client is aware of the
   * location of partitions on servers hosting {@link Region regions} with
   * {@link org.apache.geode.cache.DataPolicy#PARTITION}. Using this information, the client routes
   * the client cache operations directly to the server which is hosting the required partition for
   * the cache operation using a single network hop. This mode works best when
   * {@link #setPoolMaxConnections(int)} is set to <code>-1</code> which is the default. This mode
   * causes the client to have more connections to the servers.
   * <p>
   * If setPRSingleHopEnabled is <code>false</code> the client may need to do an extra network hop
   * on servers to go to the required partition for that cache operation. The client will use fewer
   * network connections to the servers.
   * <p>
   * Caution: for {@link org.apache.geode.cache.DataPolicy#PARTITION partition} regions with
   * {@link org.apache.geode.cache.PartitionAttributesFactory#setLocalMaxMemory(int)
   * local-max-memory} equal to zero, no cache operations mentioned above will be routed to those
   * servers as they do not host any partitions.
   *
   * @return the newly created pool.
   */
  public ClientCacheFactory setPoolPRSingleHopEnabled(boolean enabled) {
    getPoolFactory().setPRSingleHopEnabled(enabled);
    return this;
  }

  /**
   * If set to <code>true</code> then the created pool can be used by multiple users. <br>
   * <br>
   * Note: If set to true, all the client side regions must be {@link ClientRegionShortcut#PROXY
   * proxies}. No client side storage is allowed.
   *
   * @return a reference to <code>this</code>
   */
  public ClientCacheFactory setPoolMultiuserAuthentication(boolean enabled) {
    getPoolFactory().setMultiuserAuthentication(enabled);
    return this;
  }

  /**
   * Returns the version of the cache implementation.
   *
   * @return the version of the cache implementation as a <code>String</code>
   */
  public static String getVersion() {
    return GemFireVersion.getGemFireVersion();
  }

  /**
   * Gets an arbitrary open instance of {@link ClientCache} produced by an earlier call to
   * {@link #create}.
   *
   * @throws CacheClosedException if a cache has not been created or the only created one is
   *         {@link ClientCache#isClosed closed}
   * @throws IllegalStateException if the cache was created by CacheFactory instead of
   *         ClientCacheFactory
   */
  public static synchronized ClientCache getAnyInstance() {
    InternalClientCache instance = getInternalClientCache();
    if (instance == null) {
      throw new CacheClosedException(
          "A cache has not yet been created.");
    } else {
      if (!instance.isClient()) {
        throw new IllegalStateException(
            "The singleton cache was created by CacheFactory not ClientCacheFactory.");
      }
      instance.getCancelCriterion().checkCancelInProgress(null);
      return instance;
    }
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
   * @param pdxReadSerialized true to prefer PdxInstance
   * @return this ClientCacheFactory
   * @see org.apache.geode.pdx.PdxInstance
   * @since GemFire 6.6
   */
  public ClientCacheFactory setPdxReadSerialized(boolean pdxReadSerialized) {
    cacheConfig.setPdxReadSerialized(pdxReadSerialized);
    return this;
  }

  /**
   * Set the PDX serializer for the cache. If this serializer is set, it will be consulted to see if
   * it can serialize any domain classes which are added to the cache in portable data exchange
   * format.
   *
   * @param serializer the serializer to use
   * @return this ClientCacheFactory
   * @see PdxSerializer
   * @since GemFire 6.6
   */
  public ClientCacheFactory setPdxSerializer(PdxSerializer serializer) {
    cacheConfig.setPdxSerializer(serializer);
    return this;
  }

  /**
   * Set the disk store that is used for PDX meta data. When serializing objects in the PDX format,
   * the type definitions are persisted to disk. This setting controls which disk store is used for
   * that persistence.
   * <p>
   * If not set, the metadata will go in the default disk store.
   *
   * @param diskStoreName the name of the disk store to use for the PDX metadata.
   * @return this ClientCacheFactory
   * @since GemFire 6.6
   * @deprecated Pdx Persistence is not supported on client side. Even when set, it's internally
   *             ignored.
   */
  @Deprecated
  public ClientCacheFactory setPdxDiskStore(String diskStoreName) {
    cacheConfig.setPdxDiskStore(diskStoreName);
    return this;
  }

  /**
   * Control whether the type metadata for PDX objects is persisted to disk. The default for this
   * setting is false. If you are using persistent regions with PDX then you must set this to true.
   * If you are using a WAN gateway with PDX then you should set this to true.
   *
   * @param isPersistent true if the metadata should be persistent
   * @return this ClientCacheFactory
   * @since GemFire 6.6
   * @deprecated Pdx Persistence is not supported on client side. Even when set, it's internally
   *             ignored.
   */
  @Deprecated
  public ClientCacheFactory setPdxPersistent(boolean isPersistent) {
    cacheConfig.setPdxPersistent(isPersistent);
    return this;
  }

  /**
   * Control whether pdx ignores fields that were unread during deserialization. The default is to
   * preserve unread fields be including their data during serialization. But if you configure the
   * cache to ignore unread fields then their data will be lost during serialization.
   * <P>
   * You should only set this attribute to <code>true</code> if you know this member will only be
   * reading cache data. In this use case you do not need to pay the cost of preserving the unread
   * fields since you will never be reserializing pdx data.
   *
   * @param ignore <code>true</code> if fields not read during pdx deserialization should be
   *        ignored; <code>false</code>, the default, if they should be preserved.
   * @return this ClientCacheFactory
   * @since GemFire 6.6
   */
  public ClientCacheFactory setPdxIgnoreUnreadFields(boolean ignore) {
    cacheConfig.setPdxIgnoreUnreadFields(ignore);
    return this;
  }
}
