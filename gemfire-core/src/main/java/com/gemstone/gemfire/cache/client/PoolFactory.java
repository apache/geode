/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.*; // for javadocs
import com.gemstone.gemfire.cache.query.*; // for javadocs


/**
 * This interface provides for the configuration and creation of instances of
 * {@link Pool}.
 * <p>Every pool needs to have at least one {@link #addLocator locator} or {@link #addServer server} added
 * to it. Locators should be added unless direct connections to
 * cache servers are desired.
 * <p>The setter methods are used to specify
 * non-default values for the other pool properties.
 * <p>Once it is configured {@link #create}
 * will produce an instance.
 * <p>The factory can be restored to its default
 * configuration by calling {@link #reset}.
 * <p>Instances of this interface can be created by calling
 * {@link PoolManager#createFactory}.
 * <p>
 * If a subscription is going to be made using a pool then subscriptions
 * {@link #setSubscriptionEnabled must be enabled} on the pool.
 * Subscriptions are made using these APIs:
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
 * @author darrel
 * @since 5.7
 */
public interface PoolFactory {
  /**
   * The default amount of time, in milliseconds, which we will wait for a free
   * connection if max connections is set and all of the connections are in use.
   * <p>Current value: <code>10000</code>.
   */
  public static final int DEFAULT_FREE_CONNECTION_TIMEOUT = 10000;

  /**
   * The default interval in which the pool will check to see if 
   * a connection to a given server should be moved to a different
   * server to improve the load balance.
   * <p>Current value: <code>300,000</code> (which is 5 minutes).
   */
  public static final int DEFAULT_LOAD_CONDITIONING_INTERVAL = 1000*60*5;

  /**
   * Default size in bytes of the socket buffer on each connection established.
   * <p>Current value: <code>32768</code>.
   */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;

  /**
   * The default amount of time, in milliseconds, to wait for a response from a server
   * <p>Current value: <code>10000</code>.
   */
  public static final int DEFAULT_READ_TIMEOUT = 10000;

  /**
   * The default number of connections to initially create
   * <p>Current value: <code>1</code>.
   */
  public static final int DEFAULT_MIN_CONNECTIONS = 1;
  
  /**
   * The default maximum number of connections to create
   * <p>Current value: <code>-1</code>.
   */
  public static final int DEFAULT_MAX_CONNECTIONS = -1;
  
  /**
   * The default amount of time in milliseconds, to wait for a connection to become idle
   * <p>Current value: <code>5000</code>.
   */
  public static final long DEFAULT_IDLE_TIMEOUT = 5000;

  /**
   * The default number of times to retry an operation after a timeout or exception.
   * <p>Current value: <code>-1</code>.
   */
  public static final int DEFAULT_RETRY_ATTEMPTS = -1;
  
  /**
   * The default frequency, in milliseconds, to ping servers.
   * <p>Current value: <code>10000</code>.
   */
  public static final long DEFAULT_PING_INTERVAL = 10000;
  
  /**
   * The default frequency, in milliseconds, that client statistics
   * will be sent to the server.
   * <p>Current value: <code>-1</code>.
   */
  public static final int DEFAULT_STATISTIC_INTERVAL = -1;

  /**
   * The default value for whether connections should have affinity to the thread
   * that last used them.
   * <p>Current value: <code>false</code>.
   */
  public static final boolean DEFAULT_THREAD_LOCAL_CONNECTIONS = false;

  /**
   * The default value for whether to establish a server to client subscription.
   * <p>Current value: <code>false</code>.
   */
  public static final boolean DEFAULT_SUBSCRIPTION_ENABLED = false;

  /**
   * The default redundancy for servers holding subscriptions established by this
   * client
   * <p>Current value: <code>0</code>.
   */
  public static final int DEFAULT_SUBSCRIPTION_REDUNDANCY = 0;
  
  /**
   * The default amount of time, in milliseconds, that messages sent from a
   * server to a client will be tracked. The tracking is done to minimize
   * duplicate events.
   * <p>Current value: <code>900000</code>.
   */
  public static final int DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT = 900000;
  
  /**
   * The default amount of time, in milliseconds, to wait before
   * sending an acknowledgement to the server about events
   * received from the subscriptions.
   * <p>Current value: <code>100</code>.
   */
  public static final int DEFAULT_SUBSCRIPTION_ACK_INTERVAL = 100;

  /**
   * The default server group.
   * <p>Current value: <code>""</code>.
   */
  public static final String DEFAULT_SERVER_GROUP = "";

  /**
   * The default value for whether to have single hop optimisations enabled.
   * <p>Current value: <code>true</code>.
   * @since 6.5
   */
  public static final boolean DEFAULT_PR_SINGLE_HOP_ENABLED = true;
  /**
   * The default value for whether to use multiuser mode.
   * <p>Current value: <code>false</code>.
   * @since 6.5
   */  
  public static final boolean DEFAULT_MULTIUSER_AUTHENTICATION = false;
  /**
   * Sets the free connection timeout for this pool.
   * If the pool has a max connections setting, operations will block
   * if all of the connections are in use. The free connection timeout
   * specifies how long those operations will block waiting for
   * a free connection before receiving
   * an {@link AllConnectionsInUseException}. If max connections 
   * is not set this setting has no effect.
   * @see #setMaxConnections(int)
   * @param connectionTimeout the connection timeout in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionTimeout</code>
   * is less than or equal to <code>0</code>.
   */
  public PoolFactory setFreeConnectionTimeout(int connectionTimeout);
  /**
   * Sets the load conditioning interval for this pool.
   * This interval controls how frequently the pool will check to see if 
   * a connection to a given server should be moved to a different
   * server to improve the load balance.  
   * <p>A value of <code>-1</code> disables load conditioning
   * @param loadConditioningInterval the connection lifetime in milliseconds
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>connectionLifetime</code>
   * is less than <code>-1</code>.
   */
  public PoolFactory setLoadConditioningInterval(int loadConditioningInterval);
  /**
   * Sets the socket buffer size for each connection made in this pool.
   * Large messages can be received and sent faster when this buffer is larger.
   * Larger buffers also optimize the rate at which servers can send events
   * for client subscriptions.
   * @param bufferSize the size of the socket buffers used for reading and
   * writing on each connection in this pool.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>bufferSize</code>
   * is less than or equal to <code>0</code>.
   */
  public PoolFactory setSocketBufferSize(int bufferSize);
  /**
   * Sets the thread local connections policy for this pool.
   * If <code>true</code> then any time a thread goes to use a connection
   * from this pool it will check a thread local cache and see if it already
   * has a connection in it. If so it will use it. If not it will get one from
   * this pool and cache it in the thread local. This gets rid of thread contention
   * for the connections but increases the number of connections the servers see.
   * <p>If <code>false</code> then connections are returned to the pool as soon
   * as the operation being done with the connection completes. This allows
   * connections to be shared amonst multiple threads keeping the number of
   * connections down.
   * @param threadLocalConnections if <code>true</code> then enable thread local
   * connections.
   * @return a reference to <code>this</code>
   */
  public PoolFactory setThreadLocalConnections(boolean threadLocalConnections);
  
  /**
   * Sets the number of milliseconds to wait for a response from a server before
   * timing out the operation and trying another server (if any are available).
   * @param timeout number of milliseconds to wait for a response from a server
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>timeout</code>
   * is less than <code>0</code>.
   */
  public PoolFactory setReadTimeout(int timeout);
  
  /**
   * Set the minimum number of connections to keep available at all times.
   * When the pool is created, it will create this many connections. 
   * If <code>0</code> then connections will not be made until an actual operation
   * is done that requires client-to-server communication.
   * @param minConnections the initial number of connections
   * this pool will create.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>minConnections</code>
   * is less than <code>0</code>.
   */
  public PoolFactory setMinConnections(int minConnections);
  
  /**
   * Set the max number of client to server connections that the pool will create. If all of 
   * the connections are in use, an operation requiring a client to server connection
   * will block until a connection is available.
   * @see #setFreeConnectionTimeout(int) 
   * @param maxConnections the maximum number of connections in the pool.
   * this pool will create. -1 indicates that there is no maximum number of connections
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>maxConnections</code>
   * is less than <code>minConnections</code>.
   */
  public PoolFactory setMaxConnections(int maxConnections);
  
  /**
   * Set the amount of time a connection can be idle before expiring the connection.
   * If the pool size is greater than the minimum specified by 
   * {@link PoolFactory#setMinConnections(int)}, connections which have been idle
   * for longer than the idleTimeout will be closed. 
   * @param idleTimeout The amount of time in milliseconds that an idle connection
   * should live before expiring. -1 indicates that connections should never expire.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>idleTimout</code>
   * is less than <code>-1</code>.
   */
  public PoolFactory setIdleTimeout(long idleTimeout);
  
  /**
   * Set the number of times to retry a request after timeout/exception.
   * @param retryAttempts The number of times to retry a request 
   * after timeout/exception. -1 indicates that a request should be 
   * tried against every available server before failing
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>retryAttempts</code>
   * is less than <code>-1</code>.
   */
  public PoolFactory setRetryAttempts(int retryAttempts);
  
  /**
   * How often to ping servers to verify that they are still alive. Each
   * server will be sent a ping every pingInterval if there has not
   * been any other communication with the server.
   * 
   * These pings are used by the server to monitor the health of
   * the client. Make sure that the pingInterval is less than the 
   * maximum time between pings allowed by the cache server.
   * @param pingInterval The amount of time in milliseconds between
   * pings.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>pingInterval</code>
   * is less than or equal to <code>0</code>.
   * @see CacheServer#setMaximumTimeBetweenPings(int)
   */
  public PoolFactory setPingInterval(long pingInterval);

  /**
   * How often to send client statistics to the server.
   * Doing this allows <code>gfmon</code> to monitor clients.
   * <p>A value of <code>-1</code> disables the sending of client statistics
   * to the server.
   * 
   * @param statisticInterval The amount of time in milliseconds between
   * sends of client statistics to the server.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>statisticInterval</code>
   * is less than <code>-1</code>.
   */
  public PoolFactory setStatisticInterval(int statisticInterval);

  /**
   * Configures the group that all servers this pool connects to must belong to.
   * @param group the server group that this pool will connect to.
   * If <code>null</code> or <code>""</code> then all servers will be connected to.
   * @return a reference to <code>this</code>
   */
  public PoolFactory setServerGroup(String group);

  /**
   * Add a locator, given its host and port, to this factory.
   * The locator must be a server locator and will be used to discover other running
   * cache servers and locators.
   * Note that if the host is unknown at the time of this call
   * the locator will still be added. When the pool is used for
   * an operation if the host is still unknown an exception will
   * be thrown.
   * @param host the host name or ip address that the locator is listening on.
   * @param port the port that the locator is listening on
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if port is outside
   * the valid range of [0..65535] inclusive.
   * @throws IllegalStateException if a server has already been {@link #addServer added} to this factory.
   */
  public PoolFactory addLocator(String host, int port);

  /**
   * Add a server, given its host and port, to this factory.
   * The server must be a cache server and this client will
   * directly connect to without consulting a server locator.
   * Note that if the host is unknown at the time of this call
   * the server will still be added. When the pool is used for
   * an operation if the host is still unknown an exception will
   * be thrown.
   * @param host the host name or ip address that the server is listening on.
   * @param port the port that the server is listening on
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if port is outside
   * the valid range of [0..65535] inclusive.
   * @throws IllegalStateException if a locator has already been {@link #addLocator added} to this factory.
   */
  public PoolFactory addServer(String host, int port);

  /**
   * If set to <code>true</code> then the created pool will have server-to-client
   * subscriptions enabled.
   * If set to <code>false</code> then all <code>Subscription*</code> attributes
   * are ignored at create time.
   * @return a reference to <code>this</code>
   */
  public PoolFactory setSubscriptionEnabled(boolean enabled);
  
  /**
   * Sets the redundancy level for this pools server-to-client subscriptions.
   * If <code>0</code> then no redundant copies will be kept on the servers.
   * Otherwise an effort will be made to maintain the requested number of
   * copies of the server-to-client subscriptions. At most one copy per server will
   * be made up to the requested level.
   * @param redundancy the number of redundant servers for this client's subscriptions.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>redundancyLevel</code>
   * is less than <code>-1</code>.
   */
  public PoolFactory setSubscriptionRedundancy(int redundancy);
  /**
   * Sets the messageTrackingTimeout attribute which is the time-to-live period, in
   * milliseconds, for subscription events the client has received from the server. It's used
   * to minimize duplicate events.
   * Entries that have not been modified for this amount of time
   * are expired from the list
   * @param messageTrackingTimeout number of milliseconds to set the timeout to.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>messageTrackingTimeout</code>
   * is less than or equal to <code>0</code>.
   */
  public PoolFactory setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout);
  
  /**
   * Sets the interval in milliseconds
   * to wait before sending acknowledgements to the cache server for
   * events received from the server subscriptions.
   * 
   * @param ackInterval number of milliseconds to wait before sending event
   * acknowledgements.
   * @return a reference to <code>this</code>
   * @throws IllegalArgumentException if <code>ackInterval</code>
   * is less than or equal to <code>0</code>.
   */
  public PoolFactory setSubscriptionAckInterval(int ackInterval);

  /**
   * Resets the configuration of this factory to its defaults.
   * @return a reference to <code>this</code>
   */
  public PoolFactory reset();
  
  /**
   * Create a new Pool for connecting a client to a set of GemFire Cache Servers.
   * using this factory's settings for attributes.
   * 
   * @param name the name of the pool, used when connecting regions to it
   * @throws IllegalStateException if a pool with <code>name</code> already exists
   * @throws IllegalStateException if a locator or server has not been added.
   * @return the newly created pool.
   */
  public Pool create(String name);
  
  /**
   * By default setPRSingleHopEnabled is <code>true</code>
   * in which case the client is aware of the location of partitions on servers hosting
   * {@link Region regions} with
   * {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION}.
   * Using this information, the client routes the client cache operations
   * directly to the server which is hosting the required partition for the
   * cache operation using a single network hop.
   * This mode works best 
   * when {@link #setMaxConnections(int)} is set
   * to <code>-1</code> which is the default.
   * This mode causes the client to have more connections to the servers.
   * <p>
   * If setPRSingleHopEnabled is <code>false</code> the client may need to do an extra network hop on servers
   * to go to the required partition for that cache operation.
   * The client will use fewer network connections to the servers.
   * <p>
   * Caution: for {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION partition} regions
   *  with
   * {@link com.gemstone.gemfire.cache.PartitionAttributesFactory#setLocalMaxMemory(int) local-max-memory}
   * equal to zero, no cache operations mentioned above will be routed to those
   * servers as they do not host any partitions.
   * 
   * @return a reference to <code>this</code>
   * @since 6.5
   */
  public PoolFactory setPRSingleHopEnabled(boolean enabled);

  /**
   * If set to <code>true</code> then the created pool can be used by multiple
   * authenticated users. <br>
   * 
   * This setting should only be used for applications that require the client
   * to authenticate itself with the server multiple users.
   * 
   * <br>
   * Note: If set to true, all the client side regions must have their
   * data-policy set to empty.
   * 
   * @return a reference to <code>this</code>
   * @see ClientCache#createAuthenticatedView(java.util.Properties)
   * @since 6.5
   */
  public PoolFactory setMultiuserAuthentication(boolean enabled);
  
}
