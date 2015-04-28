/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

public class UserAttributes {

  private Properties credentials;
  // Update this whenever we lose/add a server.
  private ConcurrentHashMap<ServerLocation, Long> serverToId = new ConcurrentHashMap<ServerLocation, Long>();

  private Pool pool;

  public static final ThreadLocal<UserAttributes> userAttributes = new ThreadLocal<UserAttributes>();

  public UserAttributes(Properties credentials, Pool pool) {
    this.credentials = credentials;
    this.pool = pool;
  }

  public void setCredentials(Properties credentials) {
    this.credentials = credentials;
  }

  public Properties getCredentials() {
    return credentials;
  }

  public void setServerToId(ServerLocation server, Long uniqueId) {
    serverToId.put(server, uniqueId);
  }

  public ConcurrentHashMap<ServerLocation, Long> getServerToId() {
    return serverToId;
  }

  public Pool getPool() {
    return pool;
  }
}
