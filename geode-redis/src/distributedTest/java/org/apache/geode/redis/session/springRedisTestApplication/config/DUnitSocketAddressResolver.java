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

package org.apache.geode.redis.session.springRedisTestApplication.config;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.DnsResolver;
import io.lettuce.core.resource.SocketAddressResolver;

public class DUnitSocketAddressResolver extends SocketAddressResolver {

  private String[] redisPorts;
  int indexOfLastRedisPortReturned;

  public DUnitSocketAddressResolver(String[] ports) {
    super(new DnsResolver() {
      @Override
      public InetAddress[] resolve(String s) {
        return new InetAddress[0];
      }
    });

    this.redisPorts = ports;
    this.indexOfLastRedisPortReturned = 0;
  }

  @Override
  public InetSocketAddress resolve(RedisURI redisUri) {
    int redisPort =
        Integer.parseInt(redisPorts[indexOfLastRedisPortReturned++ % redisPorts.length]);

    return InetSocketAddress.createUnresolved(
        "127.0.0.1",
        redisPort);
  }
}
