/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server;

import com.gemstone.gemfire.cache.CacheCallback;


/**
 * A load probe is installed in a cache server to measure the load on the
 * cache server for balancing load between multiple cache servers.
 * 
 * <p>
 * The getLoad method will be called once per poll interval see
 * {@link CacheServer#setLoadPollInterval(long)} The {@link ServerLoad} object
 * returned by the getLoad method will be sent to the locator every time the
 * load for this server changes. To conserve bandwidth, it's a good idea to
 * round the load calculations so that the load will not be sent too frequently.
 * </p>
 * <p>
 * The {@link ServerLoad} object contains two floating point numbers indicating
 * the load on the server due to client to server connections and due to
 * subscription connections. When routing a connection, the locator will choose
 * the server that has the lowest load.
 * </p>
 * <p>
 * It is generally a good idea to pick a load function where 0 connections
 * corresponds to 0 load. The default gemfire load probes return a fraction
 * between 0 and 1, where 0 indicates no load, and 1 indicates the server is
 * completely loaded.
 * </p>
 * <p>
 * Because cache servers can be stopped, reconfigured, and restarted, the open
 * and close methods on this callback can be called several times. If the same
 * callback object is installed on multiple cache servers, open and close will
 * be called once for each cache server.
 * </p>
 * 
 * @author dsmith
 * @since 5.7
 * 
 */
public interface ServerLoadProbe extends CacheCallback {
  /**
   * Get the load on this server. This method will be called once every pool
   * interval.
   * 
   * @return The current load on this server.
   */
  ServerLoad getLoad(ServerMetrics metrics);
  
  /** Signals that a cache server
   * using this load probe has been started.
   */
  void open();
  
  /**
   * Signals that a cache server
   * using this load probe has been closed.
   */
  void close();
}
