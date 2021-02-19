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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.server.CacheServer;


/**
 * This interface provides for the configuration and creation of instances of {@link Pool}.
 * <p>
 * Every pool needs to have at least one {@link #addLocator locator} or {@link #addServer server}
 * added to it. Locators should be added unless direct connections to cache servers are desired.
 * <p>
 * The setter methods are used to specify non-default values for the other pool properties.
 * <p>
 * Once it is configured {@link #create} will produce an instance.
 * <p>
 * The factory can be restored to its default configuration by calling {@link #reset}.
 * <p>
 * Instances of this interface can be created by calling {@link PoolManager#createFactory}.
 * <p>
 * If a subscription is going to be made using a pool then subscriptions
 * {@link #setSubscriptionEnabled must be enabled} on the pool. Subscriptions are made using these
 * APIs:
 * <ul>
 * <li>{@link QueryService#newCq(String, CqAttributes)}
 * <li>{@link QueryService#newCq(String, CqAttributes, boolean)}
 * <li>{@link QueryService#newCq(String, String, CqAttributes)}
 * <li>{@link QueryService#newCq(String, String, CqAttributes, boolean)}
 * <li>{@link Region#registerInterest(Object)}
 * <li>{@link Region#registerInterest(Object, boolean)}
 * <li>{@link Region#registerInterest(Object, InterestResultPolicy)}
 * <li>{@link Region#registerInterest(Object, InterestResultPolicy, boolean)}
 * <li>{@link Region#registerInterestRegex(String)}
 * <li>{@link Region#registerInterestRegex(String, boolean)}
 * <li>{@link Region#registerInterestRegex(String, InterestResultPolicy)}
 * <li>{@link Region#registerInterestRegex(String, InterestResultPolicy, boolean)}
 * </ul>
 *
 * @since GemFire 5.7
 */
public interface PoolFactory {
  /**
   * The default amount of time, in milliseconds, socket timeout when the client connects to the
   * servers/locators.
   * <p>
   * Current value: <code>59000</code>.
   */
  int DEFAULT_SOCKET_CONNECT_TIMEOUT = 59000;

  /**
   * The default amount of time, in milliseconds, which we will wait for a free connection if max
   * connections is set and all of the connections are in use.
   * <p>
   * Current value: <code>10000</code>.
   */
  int DEFAULT_FREE_CONNECTION_TIMEOUT = 10000;

  /**
   * The default amount of time, in milliseconds, which we will wait for a server connection if max
   * connections is set and there is no free connections towards designated server.
   * <p>
   * Current value: <code>0</code>.
   */
  int DEFAULT_SERVER_CONNECTION_TIMEOUT = 0;

  /**
   * The default interval in which the pool will check to see if a connection to a given server
   * should be moved to a different server to improve the load balance.
   * <p>
   * Current value: <code>300,000</code> (which is 5 minutes).
   */
  int DEFAULT_LOAD_CONDITIONING_INTERVAL = 1000 * 60 * 5;

  /**
   * Default size in bytes of the socket buffer on each connection established.
   * <p>
   * Current value: <code>32768</code>.
   */
  int DEFAULT_SOCKET_BUFFER_SIZE = 32768;

  /**
   * The default amount of time, in milliseconds, to wait for a response from a server
   * <p>
   * Current value: <code>10000</code>.
   */
  int DEFAULT_READ_TIMEOUT = 10000;

  /**
   * The default number of connections to initially create
   * <p>
   * Current value: <code>1</code>.
   */
  int DEFAULT_MIN_CONNECTIONS = 1;

  /**
   * The default maximum number of connections to create
   * <p>
   * Current value: <code>-1</code>.
   */
  int DEFAULT_MAX_CONNECTIONS = -1;

  /**
   * The default amount of time in milliseconds, to wait for a connection to become idle
   * <p>
   * Current value: <code>5000</code>.
   */
  long DEFAULT_IDLE_TIMEOUT = 5000;

  /**
   * The default number of times to retry an operation after a timeout or exception.
   * <p>
   * Current value: <code>-1</code>.
   */
  int DEFAULT_RETRY_ATTEMPTS = -1;

  /**
   * The default frequency, in milliseconds, to ping servers.
   * <p>
   * Current value: <code>10000</code>.
   */
  long DEFAULT_PING_INTERVAL = 10000;

  /**
   * The default frequency, in milliseconds, that client statistics will be sent to the server.
   * <p>
   * Current value: <code>-1</code>.
   */
  int DEFAULT_STATISTIC_INTERVAL = -1;

  /**
   * The default value for whether connections should have affinity to the thread that last used
   * them.
   * <p>
   * Current value: <code>false</code>.
   *
   * @deprecated Since Geode 1.10.0. Thread local connections are ignored. Will be removed in future
   *             major release.
   */
  @Deprecated
  boolean DEFAULT_THREAD_LOCAL_CONNECTIONS = false;

  /**
   * The default value for whether to establish a server to client subscription.
   * <p>
   * Current value: <code>false</code>.
   */
  boolean DEFAULT_SUBSCRIPTION_ENABLED = false;

  /**
   * The default redundancy for servers holding subscriptions established by this client
   * <p>
   * Current value: <code>0</code>.
   */
  int DEFAULT_SUBSCRIPTION_REDUNDANCY = 0;

  /**
   * The default amount of time, in milliseconds, that messages sent from a server to a client will
   * be tracked. The tracking is done to minimize duplicate events.
   * <p>
   * Current value: <code>900000</code>.
   */
  int DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT = 900000;

  /**
   * The default amount of time, in milliseconds, to wait before sending an acknowledgement to the
   * server about events received from the subscriptions.
   * <p>
   * Current value: <code>100</code>.
   */
  int DEFAULT_SUBSCRIPTION_ACK_INTERVAL = 100;

  /**
   * The default number of server "ping" intervals that can elapse with no activity before a
   * subscription connection is deemed dead and failover is initiated.
   * <p>
   * Current value: 0
   */
  int DEFAULT_SUBSCRIPTION_TIMEOUT_MULTIPLIER = 0;

  /**
   * The default server group.
   * <p>
   * Current value: <code>""</code>.
   */
  String DEFAULT_SERVER_GROUP = "";

  /**
   * The default value for whether to have single hop optimisations enabled.
   * <p>
   * Current value: <code>true</code>.
   *
   * @since GemFire 6.5
   */
  boolean DEFAULT_PR_SINGLE_HOP_ENABLED = true;
  /**
   * The default value for whether to use multiuser mode.
   * <p>
   * Current value: <code>false</code>.
   *
   * @since GemFire 6.5
   */
  boolean DEFAULT_MULTIUSER_AUTHENTICATION = false;

  /**
   * The default value for the socket factory
   *
   * Current value {@link SocketFactory#DEFAULT}
   */
  @Immutable
  SocketFactory DEFAULT_SOCKET_FACTORY = SocketFactory.DEFAULT;

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
  PoolFactory setSocketConnectTimeout(int socketConnectTimeout);

  /**
   * Sets the free connection timeout for this pool. If the pool has a max connections setting,
   * operations will block if all of the connections are in use. The free connection timeout
   * specifies how long those operations will block waiting for a free connection before receiving
   * an {@link AllConnectionsInUseException}. If max connections is not set this setting has no
   * effect.
   *
   * @see #setMaxConnections(int)
   * @param connectionTimeout the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionTimeout</code> is less than or equal to
   *         <code>0</code>.
   */
  PoolFactory setFreeConnectionTimeout(int connectionTimeout);


  /**
   * Sets the server connection timeout for this pool. If the pool has a max connections setting,
   * operations will block if there is no free connection towards specific server. The server
   * connection timeout specifies how long those operations will block waiting for a free connection
   * towards specific server before receiving an {@link AllConnectionsInUseException}.
   * If max connections is not set this setting has no effect.
   * It differs from "setFreeConnectionTimeout" which sets wait time for any server connection in
   * the pool,
   * where this sets wait time for a free connection to a specific server.
   *
   * @see #setMaxConnections(int)
   * @param serverConnectionTimeout the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>serverConnectionTimeout</code> is less than
   *         <code>0</code>.
   */
  PoolFactory setServerConnectionTimeout(int serverConnectionTimeout);

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
  PoolFactory setLoadConditioningInterval(int loadConditioningInterval);

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
  PoolFactory setSocketBufferSize(int bufferSize);

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
  PoolFactory setThreadLocalConnections(boolean threadLocalConnections);

  /**
   * Sets the number of milliseconds to wait for a response from a server before timing out the
   * operation and trying another server (if any are available).
   *
   * @param timeout number of milliseconds to wait for a response from a server
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>timeout</code> is less than <code>0</code>.
   */
  PoolFactory setReadTimeout(int timeout);

  /**
   * Set the minimum number of connections to keep available at all times. When the pool is created,
   * it will create this many connections. If <code>0</code> then connections will not be made until
   * an actual operation is done that requires client-to-server communication.
   *
   * @param minConnections the initial number of connections this pool will create.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>minConnections</code> is less than <code>0</code>.
   */
  PoolFactory setMinConnections(int minConnections);

  /**
   * Set the max number of client to server connections that the pool will create. If all of the
   * connections are in use, an operation requiring a client to server connection will block until a
   * connection is available.
   *
   * @see #setFreeConnectionTimeout(int)
   * @param maxConnections the maximum number of connections in the pool. this pool will create. -1
   *        indicates that there is no maximum number of connections
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>maxConnections</code> is less than
   *         <code>minConnections</code>.
   */
  PoolFactory setMaxConnections(int maxConnections);

  /**
   * Set the amount of time a connection can be idle before expiring the connection. If the pool
   * size is greater than the minimum specified by {@link PoolFactory#setMinConnections(int)},
   * connections which have been idle for longer than the idleTimeout will be closed.
   *
   * @param idleTimeout The amount of time in milliseconds that an idle connection should live
   *        before expiring. -1 indicates that connections should never expire.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code> is less than <code>-1</code>.
   */
  PoolFactory setIdleTimeout(long idleTimeout);

  /**
   * Set the number of times to retry a request after timeout/exception.
   *
   * @param retryAttempts The number of times to retry a request after timeout/exception. -1
   *        indicates that a request should be tried against every available server before failing
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>retryAttempts</code> is less than <code>-1</code>.
   */
  PoolFactory setRetryAttempts(int retryAttempts);

  /**
   * How often to ping servers to verify that they are still alive. Each server will be sent a ping
   * every pingInterval if there has not been any other communication with the server.
   *
   * These pings are used by the server to monitor the health of the client. Make sure that the
   * pingInterval is less than the maximum time between pings allowed by the cache server.
   *
   * @param pingInterval The amount of time in milliseconds between pings.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>pingInterval</code> is less than or equal to
   *         <code>0</code>.
   * @see CacheServer#setMaximumTimeBetweenPings(int)
   */
  PoolFactory setPingInterval(long pingInterval);

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
  PoolFactory setStatisticInterval(int statisticInterval);

  /**
   * Configures the group that all servers this pool connects to must belong to.
   *
   * @param group the server group that this pool will connect to. If <code>null</code> or
   *        <code>""</code> then all servers will be connected to.
   * @return a reference to <code>this</code>
   */
  PoolFactory setServerGroup(String group);

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
   * @throws IllegalStateException if a server has already been {@link #addServer added} to this
   *         factory.
   */
  PoolFactory addLocator(String host, int port);

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
   * @throws IllegalStateException if a locator has already been {@link #addLocator added} to this
   *         factory.
   */
  PoolFactory addServer(String host, int port);

  /**
   * If set to <code>true</code> then the created pool will have server-to-client subscriptions
   * enabled. If set to <code>false</code> then all <code>Subscription*</code> attributes are
   * ignored at create time.
   *
   * @return a reference to <code>this</code>
   */
  PoolFactory setSubscriptionEnabled(boolean enabled);

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
  PoolFactory setSubscriptionRedundancy(int redundancy);

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
  PoolFactory setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout);

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
  PoolFactory setSubscriptionTimeoutMultiplier(int multiplier);

  /**
   * Sets the interval in milliseconds to wait before sending acknowledgements to the cache server
   * for events received from the server subscriptions.
   *
   * @param ackInterval number of milliseconds to wait before sending event acknowledgements.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>ackInterval</code> is less than or equal to
   *         <code>0</code>.
   */
  PoolFactory setSubscriptionAckInterval(int ackInterval);

  /**
   * Resets the configuration of this factory to its defaults.
   *
   * @return a reference to <code>this</code>
   */
  PoolFactory reset();

  /**
   * Create a new Pool for connecting a client to a set of GemFire Cache Servers. using this
   * factory's settings for attributes.
   *
   * @param name the name of the pool, used when connecting regions to it
   * @throws IllegalStateException if a pool with <code>name</code> already exists
   * @throws IllegalStateException if a locator or server has not been added.
   * @return the newly created pool.
   */
  Pool create(String name);

  /**
   * By default setPRSingleHopEnabled is <code>true</code> in which case the client is aware of the
   * location of partitions on servers hosting {@link Region regions} with
   * {@link org.apache.geode.cache.DataPolicy#PARTITION}. Using this information, the client routes
   * the client cache operations directly to the server which is hosting the required partition for
   * the cache operation using a single network hop. This mode works best when
   * {@link #setMaxConnections(int)} is set to <code>-1</code> which is the default. This mode
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
   * @return a reference to <code>this</code>
   * @since GemFire 6.5
   */
  PoolFactory setPRSingleHopEnabled(boolean enabled);

  /**
   * If set to <code>true</code> then the created pool can be used by multiple authenticated users.
   * <br>
   *
   * This setting should only be used for applications that require the client to authenticate
   * itself with the server multiple users.
   *
   * <br>
   * Note: If set to true, all the client side regions must have their data-policy set to empty.
   *
   * @return a reference to <code>this</code>
   * @see ClientCache#createAuthenticatedView(java.util.Properties)
   * @since GemFire 6.5
   */
  PoolFactory setMultiuserAuthentication(boolean enabled);

  /**
   * Set the socket factory used by this pool to create connections to both locators (if
   * configured using {@link #addLocator(String, int)}) and servers.
   *
   * see {@link SocketFactory}
   * See {@link ProxySocketFactories}
   *
   * @param socketFactory The {@link SocketFactory} to use
   * @return a reference to <code> this </code>
   * @since Geode 1.13
   */
  PoolFactory setSocketFactory(SocketFactory socketFactory);

}
