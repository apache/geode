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

package org.apache.geode.redis.internal;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.inet.LocalHostUtil;

public class SystemPropertyBasedRedisConfiguration implements RedisConfiguration {

  public static final String GEODE_FOR_REDIS_PORT = "gemfire.geode-for-redis-port";

  public static final String GEODE_FOR_REDIS_BIND_ADDRESS = "gemfire.geode-for-redis-bind-address";

  public static final String GEODE_FOR_REDIS_REDUNDANT_COPIES =
      "gemfire.geode-for-redis-redundant-copies";

  public static final String GEODE_FOR_REDIS_USERNAME = "gemfire.geode-for-redis-username";

  public static final String GEODE_FOR_REDIS_ENABLED = "gemfire.geode-for-redis-enabled";

  private int port;
  private String bindAddress;
  private int redundantCopies;
  private String username;
  private final boolean enabled;

  public SystemPropertyBasedRedisConfiguration() throws IllegalArgumentException {
    boolean tmpEnabled = validateAndSetPort();
    tmpEnabled |= validateAndSetBindAddress();
    tmpEnabled |= validateAndSetRedundantCopies();
    tmpEnabled |= validateAndSetUsername();
    tmpEnabled |= Boolean.getBoolean(GEODE_FOR_REDIS_ENABLED);

    enabled = tmpEnabled;
  }

  public static SystemPropertyBasedRedisConfiguration generate() throws IllegalArgumentException {
    return new SystemPropertyBasedRedisConfiguration();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getPort() {
    return port;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public int getRedundantCopies() {
    return redundantCopies;
  }

  public String getUsername() {
    return username;
  }

  private boolean validateAndSetPort() {
    String portString = System.getProperty(GEODE_FOR_REDIS_PORT);
    if (StringUtils.isEmpty(portString)) {
      port = DEFAULT_REDIS_PORT;
      return false;
    }

    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(GEODE_FOR_REDIS_PORT + " is invalid: " + portString);
    }

    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException(GEODE_FOR_REDIS_PORT + " is out of range (0..65535): "
          + port);
    }

    return true;
  }

  private boolean validateAndSetRedundantCopies() {
    String copiesString = System.getProperty(GEODE_FOR_REDIS_REDUNDANT_COPIES);
    if (StringUtils.isEmpty(copiesString)) {
      redundantCopies = DEFAULT_REDIS_REDUNDANT_COPIES;
      return false;
    }

    try {
      redundantCopies = Integer.parseInt(copiesString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          GEODE_FOR_REDIS_REDUNDANT_COPIES + " is invalid: " + copiesString);
    }

    if (redundantCopies < 0 || redundantCopies > 3) {
      throw new IllegalArgumentException(GEODE_FOR_REDIS_REDUNDANT_COPIES +
          " is out of range (0..3): " + redundantCopies);
    }

    return true;
  }

  private boolean validateAndSetBindAddress() {
    String value = System.getProperty(GEODE_FOR_REDIS_BIND_ADDRESS);
    if (StringUtils.isEmpty(value)) {
      bindAddress = DEFAULT_REDIS_BIND_ADDRESS;
      return false;
    }

    if (!LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The geode-for-redis-bind-address %s is not a valid address for this machine. These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }

    bindAddress = value;
    return true;
  }

  private boolean validateAndSetUsername() {
    String value = System.getProperty(GEODE_FOR_REDIS_USERNAME);
    if (StringUtils.isEmpty(value)) {
      username = DEFAULT_REDIS_USERNAME;
      return false;
    }

    username = value;
    return true;
  }
}
