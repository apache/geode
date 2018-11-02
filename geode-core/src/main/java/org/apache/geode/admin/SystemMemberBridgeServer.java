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
package org.apache.geode.admin;

import org.apache.geode.cache.server.ServerLoadProbe;

/**
 * Administrative interface that represents a CacheServer that serves the contents of a system
 * member's cache.
 *
 * @see SystemMemberCache#addCacheServer
 *
 * @since GemFire 4.0
 * @deprecated as of 5.7 use {@link SystemMemberCacheServer} instead.
 */
@Deprecated
public interface SystemMemberBridgeServer {

  /**
   * Returns the port on which this cache server listens for bridge clients to connect.
   */
  int getPort();

  /**
   * Sets the port on which this cache server listens for bridge clients to connect.
   *
   * @throws AdminException If this cache server is running
   */
  void setPort(int port) throws AdminException;

  /**
   * Starts this cache server. Once the server is running, its configuration cannot be changed.
   *
   * @throws AdminException If an error occurs while starting the cache server
   */
  void start() throws AdminException;

  /**
   * Returns whether or not this cache server is running
   */
  boolean isRunning();

  /**
   * Stops this cache server. Note that the <code>BridgeServer</code> can be reconfigured and
   * restarted if desired.
   */
  void stop() throws AdminException;

  /**
   * Updates the information about this cache server.
   */
  void refresh();

  /**
   * Returns a string representing the ip address or host name that this server will listen on.
   *
   * @return the ip address or host name that this server is to listen on
   * @since GemFire 5.7
   */
  String getBindAddress();

  /**
   * Sets the ip address or host name that this server is to listen on for client connections.
   * <p>
   * Setting a specific bind address will cause the cache server to always use this address and
   * ignore any address specified by "server-bind-address" or "bind-address" in the
   * <code>gemfire.properties</code> file (see
   * {@link org.apache.geode.distributed.DistributedSystem} for a description of these properties).
   * <p>
   * A <code>null</code> value will be treated the same as the default "".
   * <p>
   * The default value does not override the gemfire.properties. If you wish to override the
   * properties and want to have your server bind to all local addresses then use this string
   * <code>"0.0.0.0"</code>.
   *
   * @param address the ip address or host name that this server is to listen on
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setBindAddress(String address) throws AdminException;

  /**
   * Returns a string representing the ip address or host name that server locators will tell
   * clients that this server is listening on.
   *
   * @return the ip address or host name to give to clients so they can connect to this server
   * @since GemFire 5.7
   */
  String getHostnameForClients();

  /**
   * Sets the ip address or host name that this server is to listen on for client connections.
   * <p>
   * Setting a specific hostname-for-clients will cause server locators to use this value when
   * telling clients how to connect to this server.
   * <p>
   * The default value causes the bind-address to be given to clients
   * <p>
   * A <code>null</code> value will be treated the same as the default "".
   *
   * @param name the ip address or host name that will be given to clients so they can connect to
   *        this server
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setHostnameForClients(String name) throws AdminException;

  /**
   * Sets whether or not this cache server should notify clients based on key subscription.
   *
   * If false, then an update to any key on the server causes an update to be sent to all clients.
   * This update does not push the actual data to the clients. Instead, it causes the client to
   * locally invalidate or destroy the corresponding entry. The next time the client requests the
   * key, it goes to the cache server for the value.
   *
   * If true, then an update to any key on the server causes an update to be sent to only those
   * clients who have registered interest in that key. Other clients are not notified of the change.
   * In addition, the actual value is pushed to the client. The client does not need to request the
   * new value from the cache server.
   *
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setNotifyBySubscription(boolean b) throws AdminException;

  /**
   * Answers whether or not this cache server should notify clients based on key subscription.
   *
   * @since GemFire 5.7
   */
  boolean getNotifyBySubscription();

  /**
   * Sets the buffer size in bytes of the socket connection for this <code>BridgeServer</code>. The
   * default is 32768 bytes.
   *
   * @param socketBufferSize The size in bytes of the socket buffer
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setSocketBufferSize(int socketBufferSize) throws AdminException;

  /**
   * Returns the configured buffer size of the socket connection for this <code>BridgeServer</code>.
   * The default is 32768 bytes.
   *
   * @return the configured buffer size of the socket connection for this <code>BridgeServer</code>
   * @since GemFire 5.7
   */
  int getSocketBufferSize();

  /**
   * Sets the maximum amount of time between client pings. This value is used by the
   * <code>ClientHealthMonitor</code> to determine the health of this <code>BridgeServer</code>'s
   * clients. The default is 60000 ms.
   *
   * @param maximumTimeBetweenPings The maximum amount of time between client pings
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) throws AdminException;

  /**
   * Returns the maximum amount of time between client pings. This value is used by the
   * <code>ClientHealthMonitor</code> to determine the health of this <code>BridgeServer</code>'s
   * clients. The default is 60000 ms.
   *
   * @return the maximum amount of time between client pings.
   * @since GemFire 5.7
   */
  int getMaximumTimeBetweenPings();

  /**
   * Returns the maximum allowed client connections
   *
   * @since GemFire 5.7
   */
  int getMaxConnections();

  /**
   * Sets the maxium number of client connections allowed. When the maximum is reached the server
   * will stop accepting connections.
   *
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setMaxConnections(int maxCons) throws AdminException;

  /**
   * Returns the maxium number of threads allowed in this server to service client requests. The
   * default of <code>0</code> causes the server to dedicate a thread for every client connection.
   *
   * @since GemFire 5.7
   */
  int getMaxThreads();

  /**
   * Sets the maxium number of threads allowed in this server to service client requests. The
   * default of <code>0</code> causes the server to dedicate a thread for every client connection.
   *
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setMaxThreads(int maxThreads) throws AdminException;

  /**
   * Returns the maximum number of messages that can be enqueued in a client-queue.
   *
   * @since GemFire 5.7
   */
  int getMaximumMessageCount();

  /**
   * Sets maximum number of messages that can be enqueued in a client-queue.
   *
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setMaximumMessageCount(int maxMessageCount) throws AdminException;

  /**
   * Returns the time (in seconds ) after which a message in the client queue will expire.
   *
   * @since GemFire 5.7
   */
  int getMessageTimeToLive();

  /**
   * Sets the time (in seconds ) after which a message in the client queue will expire.
   *
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setMessageTimeToLive(int messageTimeToLive) throws AdminException;

  /**
   * Sets the list of server groups this cache server will belong to. By default cache servers
   * belong to the default global server group which all cache servers always belong to.
   *
   * @param groups possibly empty array of <code>String</code> where each string is a server groups
   *        that this cache server will be a member of.
   * @throws AdminException if this cache server is running
   * @since GemFire 5.7
   */
  void setGroups(String[] groups) throws AdminException;

  /**
   * Returns the list of server groups that this cache server belongs to.
   *
   * @return a possibly empty array of <code>String</code>s where each string is a server group.
   *         Modifying this array will not change the server groups that this cache server belongs
   *         to.
   * @since GemFire 5.7
   */
  String[] getGroups();

  /**
   * Get a description of the load probe for this cache server. {@link ServerLoadProbe} for details
   * on the load probe.
   *
   * @return the load probe used by this cache server.
   * @since GemFire 5.7
   */
  String getLoadProbe();

  /**
   * Set the load probe for this cache server. See {@link ServerLoadProbe} for details on how to
   * implement a load probe.
   *
   * The load probe should implement DataSerializable if it is used with this interface, because it
   * will be sent to the remote VM.
   *
   * @param loadProbe the load probe to use for this cache server.
   * @throws AdminException if the cache server is running
   * @since GemFire 5.7
   */
  void setLoadProbe(ServerLoadProbe loadProbe) throws AdminException;

  /**
   * Get the frequency in milliseconds to poll the load probe on this cache server.
   *
   * @return the frequency in milliseconds that we will poll the load probe.
   */
  long getLoadPollInterval();

  /**
   * Set the frequency in milliseconds to poll the load probe on this cache server
   *
   * @param loadPollInterval the frequency in milliseconds to poll the load probe. Must be greater
   *        than 0.
   * @throws AdminException if the cache server is running
   */
  void setLoadPollInterval(long loadPollInterval) throws AdminException;

}
