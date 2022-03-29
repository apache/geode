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

package org.apache.geode.redis;

import org.apache.geode.redis.internal.RedisConfiguration;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class TestRedisConfiguration implements RedisConfiguration {

  private final String bindAddress;
  private final int port;
  private final int redundantCopies;
  private final String username;

  public static class Builder {
    private String address = RedisClusterStartupRule.BIND_ADDRESS;
    private int port = RedisConfiguration.DEFAULT_REDIS_PORT;
    private int redundantCopies = RedisConfiguration.DEFAULT_REDIS_REDUNDANT_COPIES;
    private String username = RedisConfiguration.DEFAULT_REDIS_USERNAME;

    private Builder() {}

    public RedisConfiguration build() {
      return new TestRedisConfiguration(address, port, redundantCopies, username);
    }

    public Builder withAddress(String address) {
      this.address = address;
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    public Builder withRedundantCopies(int redundantCopies) {
      this.redundantCopies = redundantCopies;
      return this;
    }

    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }
  }

  private TestRedisConfiguration(String bindAddress, int port, int redundantCopies,
      String username) {
    this.bindAddress = bindAddress;
    this.port = port;
    this.redundantCopies = redundantCopies;
    this.username = username;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getBindAddress() {
    return bindAddress;
  }

  @Override
  public int getRedundantCopies() {
    return 0;
  }

  @Override
  public String getUsername() {
    return username;
  }
}
