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

/**
 * Interface that defines access to 'official' properties for Geode for Redis
 */
public interface RedisConfiguration {

  /**
   * The default Geode for Redis port. Can be set using
   * {@link SystemPropertyBasedRedisConfiguration#GEODE_FOR_REDIS_PORT}.
   */
  int DEFAULT_REDIS_PORT = 6379;

  /**
   * The default Geode for Redis bind address. Can be set using
   * {@link SystemPropertyBasedRedisConfiguration#GEODE_FOR_REDIS_BIND_ADDRESS}.
   */
  String DEFAULT_REDIS_BIND_ADDRESS = "127.0.0.1";

  /**
   * The default Geode for Redis redundant copies. Must be in the range (0..3). Can be set using
   * {@link SystemPropertyBasedRedisConfiguration#GEODE_FOR_REDIS_REDUNDANT_COPIES}.
   */
  int DEFAULT_REDIS_REDUNDANT_COPIES = 1;

  /**
   * The default Geode for Redis username. Can be set using
   * {@link SystemPropertyBasedRedisConfiguration#GEODE_FOR_REDIS_USERNAME}.
   */
  String DEFAULT_REDIS_USERNAME = "default";

  boolean isEnabled();

  int getPort();

  String getBindAddress();

  int getRedundantCopies();

  String getUsername();

}
