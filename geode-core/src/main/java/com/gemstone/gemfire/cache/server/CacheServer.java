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
package com.gemstone.gemfire.cache.server;

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.ClientSession;
import com.gemstone.gemfire.cache.InterestRegistrationListener;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.cache.server.internal.ConnectionCountProbe;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * A cache server that serves the contents of a
 * <code>Cache</code> to client VMs in another distributed system via a
 * socket.  A cache server is used in conjunction with a
 * client {@link Pool} to connect two regions
 * that reside in different distributed systems.
 *
 * @see com.gemstone.gemfire.cache.Cache#addCacheServer
 * @see com.gemstone.gemfire.cache.Cache#getCacheServers
 *
 * @since GemFire 5.7
 */
public interface CacheServer {
  /** The default port on which a <Code>CacheServer</code> is
   * configured to serve. */
  public static final int DEFAULT_PORT = 40404;

  /** 
   * The default number of sockets accepted by a CacheServer. 
   * When the maximum is reached the cache server will stop accepting new connections.
   * Current value: 800
   * @since GemFire 5.7
   */
  public static final int DEFAULT_MAX_CONNECTIONS = 800;
  // Value derived from common file descriptor limits for Unix sytems (1024)... 

  /** 
   * The default limit to the maximum number of cache server threads that can be
   * created to service client requests. Once this number of threads exist then
   * connections must share the same thread to service their request. A selector
   * is used to detect client connection requests and dispatch them to the thread
   * pool.
   * The default of <code>0</code> causes a thread to be bound to every connection
   * and to be dedicated to detecting client requests on that connection. A selector
   * is not used in this default mode.
   * Current value: 0
   * @since GemFire 5.7
   */
  public static final int DEFAULT_MAX_THREADS = 0;

  /** The default notify-by-subscription value which tells the 
   * <Code>CacheServer</code> whether or not to notify clients 
   * based on key subscription.
   */ 
  public static final boolean DEFAULT_NOTIFY_BY_SUBSCRIPTION = true; 
 
  /**
   * The default socket buffer size for socket buffers from the cache server
   * to the client.
   */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;

  /**
   * The default maximum amount of time between client pings. This value
   * is used by the <code>ClientHealthMonitor</code> to determine the
   * health of this <code>CacheServer</code>'s clients.
   */
  public static final int DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS = 60000;

  /**
   * The default maximum number of messages that can be enqueued in a
   * client-queue.
   */
  public static final int DEFAULT_MAXIMUM_MESSAGE_COUNT = 230000;

  /**
   * The default time (in seconds ) after which a message in the client queue
   * will expire.
   */
  public static final int DEFAULT_MESSAGE_TIME_TO_LIVE = 180;
  
  /**
   * The default list of server groups a cache server belongs to.
   * The current default is an empty list.
   * @since GemFire 5.7
   * @deprecated as of 7.0 use the groups gemfire property
   */
  public static final String[] DEFAULT_GROUPS = new String[0];
  
  /**
   * The default load balancing probe. The default load balancing
   * probe reports the connections counts of this cache server. 
   * @since GemFire 5.7
   *  
   */
  public static final ServerLoadProbe DEFAULT_LOAD_PROBE = new ConnectionCountProbe();
  
  /**
   * The default frequency at which to poll the load probe for the load
   * on this cache server. Defaults to 5000 (5 seconds).
   * @since GemFire 5.7
   */
  public static final long DEFAULT_LOAD_POLL_INTERVAL = 5000;

  /**
   * The default ip address or host name that the cache server's socket will
   * listen on for client connections.
   * The current default is an empty string.
   * @since GemFire 5.7
   */
  public static final String DEFAULT_BIND_ADDRESS = "";

  /**
   * The default ip address or host name that will be given to clients
   * as the host this cache server is listening on.
   * The current default is an empty string.
   * @since GemFire 5.7
   */
  public static final String DEFAULT_HOSTNAME_FOR_CLIENTS = "";
  
  /**
   * The default setting for outgoing tcp/ip connections.  By
   * default the product enables tcpNoDelay.
   */
  public static final boolean DEFAULT_TCP_NO_DELAY = true;

  /**
   * Returns the port on which this cache server listens for clients.
   */
  public int getPort();

  /**
   * Sets the port on which this cache server listens for clients.
   *
   * @throws IllegalStateException
   *         If this cache server is running
   */
  public void setPort(int port);

  /**
   * Returns a string representing the ip address or host name that this cache server
   * will listen on.
   * @return the ip address or host name that this cache server is to listen on
   * @see #DEFAULT_BIND_ADDRESS
   * @since GemFire 5.7
   */
  public String getBindAddress();
  /**
   * Sets the ip address or host name that this cache server is to listen on for
   * client connections.
   * <p>Setting a specific bind address will cause the cache server to always
   * use this address and ignore any address specified by "server-bind-address"
   * or "bind-address" in the <code>gemfire.properties</code> file
   * (see {@link com.gemstone.gemfire.distributed.DistributedSystem}
   * for a description of these properties).
   * <p> The value <code>""</code> does not override the <code>gemfire.properties</code>.
   * It will cause the local machine's default address to be listened on if the
   * properties file does not specify and address.
   * If you wish to override the properties and want to have your cache server bind to all local
   * addresses then use this bind address <code>"0.0.0.0"</code>.
   * <p> A <code>null</code> value will be treated the same as the default <code>""</code>.
   * @param address the ip address or host name that this cache server is to listen on
   * @see #DEFAULT_BIND_ADDRESS
   * @since GemFire 5.7
   */
  public void setBindAddress(String address);
  /**
   * Returns a string representing the ip address or host name that server locators
   * will tell clients that this cache server is listening on.
   * @return the ip address or host name to give to clients so they can connect
   *         to this cache server
   * @see #DEFAULT_HOSTNAME_FOR_CLIENTS
   * @since GemFire 5.7
   */
  public String getHostnameForClients();
  /**
   * Sets the ip address or host name that this cache server is to listen on for
   * client connections.
   * <p>Setting a specific hostname-for-clients will cause server locators
   * to use this value when telling clients how to connect to this cache server.
   * This is useful in the case where the cache server may refer to itself with one
   * hostname, but the clients need to use a different hostname to find the 
   * cache server.
   * <p> The value <code>""</code> causes the <code>bind-address</code> to be given to clients.
   * <p> A <code>null</code> value will be treated the same as the default <code>""</code>.
   * @param name the ip address or host name that will be given to clients
   *   so they can connect to this cache server
   * @see #DEFAULT_HOSTNAME_FOR_CLIENTS
   * @since GemFire 5.7
   */
  public void setHostnameForClients(String name);
  /**
   * Sets whether or not this cache server should notify clients based on
   * key subscription.
   *
   * If false, then an update to any key on the cache server causes an update to
   * be sent to all clients. This update does not push the actual data to the
   * clients. Instead, it causes the client to locally invalidate or destroy
   * the corresponding entry. The next time the client requests the key, it
   * goes to the cache server for the value.
   *
   * If true, then an update to any key on the cache server causes an update to be
   * sent to only those clients who have registered interest in that key. Other
   * clients are not notified of the change. In addition, the actual value is
   * pushed to the client. The client does not need to request the new value
   * from the cache server.
   *
   * @since GemFire 4.2
   * @deprecated as of 6.0.1. This method is no longer in use, by default 
   * notifyBySubscription attribute is set to true.
   */
  @Deprecated
  public void setNotifyBySubscription(boolean b);

  /**
   * Answers whether or not this cache server should notify clients based on
   * key subscription.
   *
   * @since GemFire 4.2
   * @deprecated as of 6.0.1. This method is no more in use, by default 
   * notifyBySubscription attribute is set to true.
   */
  @Deprecated
  public boolean getNotifyBySubscription();

  /**
   * Sets the buffer size in bytes of the socket connection for this
   * <code>CacheServer</code>. The default is 32768 bytes.
   *
   * @param socketBufferSize The size in bytes of the socket buffer
   *
   * @since GemFire 4.2.1
   */
  public void setSocketBufferSize(int socketBufferSize);

  /**
   * Returns the configured buffer size of the socket connection for this
   * <code>CacheServer</code>. The default is 32768 bytes.
   * @return the configured buffer size of the socket connection for this
   * <code>CacheServer</code>
   *
   * @since GemFire 4.2.1
   */
  public int getSocketBufferSize();

  /**
   * Sets the maximum amount of time between client pings. This value is
   * used by the <code>ClientHealthMonitor</code> to determine the health
   * of this <code>CacheServer</code>'s clients. The default is 60000 ms.
   *
   * @param maximumTimeBetweenPings The maximum amount of time between client
   * pings
   *
   * @since GemFire 4.2.3
   */
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings);

  /**
   * Returns the maximum amount of time between client pings. This value is
   * used by the <code>ClientHealthMonitor</code> to determine the health
   * of this <code>CacheServer</code>'s clients. The default is 60000 ms.
   * @return the maximum amount of time between client pings.
   *
   * @since GemFire 4.2.3
   */
  public int getMaximumTimeBetweenPings();

  /**
   * Starts this cache server.  Once the cache server is running, its
   * configuration cannot be changed.
   *
   * @throws IOException
   *         If an error occurs while starting the cache server
   */
  public void start() throws IOException;

  /**
   * Returns whether or not this cache server is running
   */
  public boolean isRunning();

  /**
   * Stops this cache server.  Note that the
   * <code>CacheServer</code> can be reconfigured and restarted if
   * desired.
   */
  public void stop();

  /** 
   *  Returns the maximum allowed client connections
   */
  public int getMaxConnections();

  /**
   * Sets the maxium number of client connections allowed.
   * When the maximum is reached the cache server will stop accepting
   * connections.
   * 
   * @see #DEFAULT_MAX_CONNECTIONS
   */
  public void setMaxConnections(int maxCons);

  /** 
   * Returns the maxium number of threads allowed in this cache server to service
   * client requests.
   * The default of <code>0</code> causes the cache server to dedicate a thread for
   * every client connection.
   * @since GemFire 5.1
   */
  public int getMaxThreads();

  /**
   * Sets the maxium number of threads allowed in this cache server to service
   * client requests.
   * The default of <code>0</code> causes the cache server to dedicate a thread for
   * every client connection.
   * 
   * @see #DEFAULT_MAX_THREADS
   * @since GemFire 5.1
   */
  public void setMaxThreads(int maxThreads);

  /**
   * Returns the maximum number of messages that can be enqueued in a
   * client-queue.
   */
  public int getMaximumMessageCount();

  /**
   * Sets maximum number of messages that can be enqueued in a client-queue.
   * 
   * @see #DEFAULT_MAXIMUM_MESSAGE_COUNT
   */
  public void setMaximumMessageCount(int maxMessageCount);
  
  /**
   * Returns the time (in seconds ) after which a message in the client queue
   * will expire.
   */
  public int getMessageTimeToLive();

  /**
   * Sets the time (in seconds ) after which a message in the client queue
   * will expire.
   * 
   * @see #DEFAULT_MESSAGE_TIME_TO_LIVE
   */
  public void setMessageTimeToLive(int messageTimeToLive);

  /**
   * Sets the list of server groups this cache server will belong to.
   * By default cache servers belong to the default global server group
   * which all cache servers always belong to.
   * @param groups possibly empty array of <code>String</code> where each string
   * is a server groups that this cache server will be a member of.
   * @see #DEFAULT_GROUPS
   * @since GemFire 5.7
   * @deprecated as of 7.0 use the groups gemfire property
   */
  public void setGroups(String[] groups);
  /**
   * Returns the list of server groups that this cache server belongs to.
   * @return a possibly empty array of <code>String</code>s where
   * each string is a server group. Modifying this array will not change the
   * server groups that this cache server belongs to.
   * @since GemFire 5.7
   * @deprecated as of 7.0 use the groups gemfire property
   */
  public String[] getGroups();

  /**
   * Get the load probe for this cache server. See
   * {@link ServerLoadProbe} for details on the load probe.
   * @return the load probe used by this cache
   * server.
   * @since GemFire 5.7
   */
  public ServerLoadProbe getLoadProbe();

  /**
   * Set the load probe for this cache server. See
   * {@link ServerLoadProbe} for details on how to implement
   * a load probe.
   * @param loadProbe the load probe to use for
   * this cache server.
   * @since GemFire 5.7
   */
  public void setLoadProbe(ServerLoadProbe loadProbe);

  /**
   * Get the frequency in milliseconds to poll the load probe on this cache
   * server.
   * 
   * @return the frequency in milliseconds that we will poll the load probe.
   */
  public long getLoadPollInterval();

  /**
   * Set the frequency in milliseconds to poll the load probe on this cache
   * server
   * @param loadPollInterval the frequency in milliseconds to poll
   * the load probe. Must be greater than 0.
   */
  public void setLoadPollInterval(long loadPollInterval);
  
  /**
   * Get the outgoing connection tcp-no-delay setting.  If it is set to true
   * (the default) this cache server is configured to enable tcp-no-delay on outgoing
   * tcp/ip sockets.  If it is set to false this cache server
   * is configured to disable tcp-no-delay on outgoing sockets.
   * 
   * @return the tcp-no-delay setting
   */
  public boolean getTcpNoDelay();
  
  /**
   * Configures the tcpNoDelay setting of sockets used to send messages
   * to clients.  TcpNoDelay is enabled by default.
   * 
   * @param noDelay if false, sets tcp-no-delay to false on out-going connections
   */
  public void setTcpNoDelay(boolean noDelay);
  
  /**
   * Get the ClientSubscriptionConfig for this cache server. See
   * {@link ClientSubscriptionConfig} for details on the client subscription configuration.
   * 
   * @return ClientSubscriptionConfig
   * @since GemFire 5.7
   */
  public ClientSubscriptionConfig getClientSubscriptionConfig();
  
  /**
   * Returns the <code>ClientSession</code> associated with the
   * <code>DistributedMember</code>
   * @return the <code>ClientSession</code> associated with the
   * <code>DistributedMember</code>
   * @since GemFire 6.0
   */
  public ClientSession getClientSession(DistributedMember member);
  
  /**
   * Returns the <code>ClientSession</code> associated with the
   * durable client id
   * @return the <code>ClientSession</code> associated with the
   * durable
   * @since GemFire 6.0
   */
  public ClientSession getClientSession(String durableClientId);
  
  /**
   * Returns a set of all <code>ClientSession</code>s
   * @return a set of all <code>ClientSession</code>s
   * @since GemFire 6.0
   */
  public Set<ClientSession> getAllClientSessions();

  /** 
   * Registers a new <code>InterestRegistrationListener</code> with the set of 
   * <code>InterestRegistrationListener</code>s. 
   * 
   * @param listener 
   *          The <code>InterestRegistrationListener</code> to register 
   * 
   * @since GemFire 6.0
   */ 
  public void registerInterestRegistrationListener( 
      InterestRegistrationListener listener); 

  /** 
   * Unregisters an existing <code>InterestRegistrationListener</code> from 
   * the set of <code>InterestRegistrationListener</code>s. 
   * 
   * @param listener 
   *          The <code>InterestRegistrationListener</code> to unregister 
   * 
   * @since GemFire 6.0
   */ 
  public void unregisterInterestRegistrationListener( 
      InterestRegistrationListener listener); 

  /** 
   * Returns a read-only set of <code>InterestRegistrationListener</code>s 
   * registered with this notifier. 
   * 
   * @return a read-only set of <code>InterestRegistrationListener</code>s 
   *         registered with this notifier 
   * 
   * @since GemFire 6.0
   */ 
  public Set<InterestRegistrationListener> getInterestRegistrationListeners(); 
  
}
