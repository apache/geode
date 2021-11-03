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

package org.apache.geode.cache.client.internal;

import static java.lang.ThreadLocal.withInitial;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.client.Pool;
import org.apache.geode.distributed.internal.ServerLocation;

/**
 * An instance of the class is created per ProxyCache/RegionService
 */
public class UserAttributes {

  private Properties credentials;

  /**
   * Update this whenever we lose/add a server.
   */
  private final ConcurrentHashMap<ServerLocation, Long> serverToId = new ConcurrentHashMap<>();

  private final Pool pool;

  /**
   * Intentionally {@code null} initially to indicate no {@link UserAttributes} associated with
   * this thread.
   */
  public static final ThreadLocal<UserAttributes> userAttributes = withInitial(() -> null);

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
