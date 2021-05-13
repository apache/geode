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

package org.apache.geode.redis.internal.cluster;

import java.net.InetAddress;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.inet.LocalHostUtil;

public class RedisMemberInfoRetrievalFunction implements InternalFunction<Void> {

  public static final String ID = RedisMemberInfoRetrievalFunction.class.getName();
  private static final long serialVersionUID = 2207969011229079993L;

  private final String hostAddress;
  private final int redisPort;

  private RedisMemberInfoRetrievalFunction(String address, int redisPort) {
    if (address == null || address.isEmpty() || address.equals("0.0.0.0")) {
      InetAddress localhost = null;
      try {
        localhost = LocalHostUtil.getLocalHost();
      } catch (Exception ignored) {
      }
      hostAddress = localhost == null ? "127.0.0.1" : localhost.getHostAddress();
    } else {
      hostAddress = address;
    }

    this.redisPort = redisPort;
  }

  public static void register(String address, int redisPort) {
    FunctionService.registerFunction(new RedisMemberInfoRetrievalFunction(address, redisPort));
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    DistributedMember member = context.getCache().getDistributedSystem().getDistributedMember();
    context.getResultSender().lastResult(new RedisMemberInfo(member, hostAddress, redisPort));
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
