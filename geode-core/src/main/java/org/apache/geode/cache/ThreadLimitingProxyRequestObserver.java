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
package org.apache.geode.cache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ThreadLimitingProxyRequestObserver
    implements ProxyRequestObserver {
  private final int maxThreadsToDestination;

  public ThreadLimitingProxyRequestObserver(int maxThreadsToDestination) {
    this.maxThreadsToDestination = maxThreadsToDestination;
  }

  private static final Logger logger = LogService.getLogger();
  private final Map<InternalDistributedMember, Integer> threadsToDestination =
      new ConcurrentHashMap();

  @Override
  public void beforeSendRequest(Set<InternalDistributedMember> members) {
    if (Thread.currentThread().getName().startsWith("ServerConnection on")
        || Thread.currentThread().getName().startsWith("Function Execution Processor")) {
      for (InternalDistributedMember member : members) {
        if (threadsToDestination.getOrDefault(member, 0) >= maxThreadsToDestination) {
          CacheFactory.getAnyInstance().getCacheServers().get(0).incRejectedProxyRequests();
          logger.info("toberal Max number of threads reached for " + member, new Exception("kk"));
          throw new IllegalStateException("Max number of threads reached");
        }
      }
      for (InternalDistributedMember member : members) {
        threadsToDestination.merge(member, 1, Integer::sum);
      }
    }
  }

  @Override
  public void afterReceiveResponse(Set<InternalDistributedMember> members) {
    for (InternalDistributedMember member : members) {
      if (Thread.currentThread().getName().startsWith("ServerConnection on")
          || Thread.currentThread().getName().startsWith("Function Execution Processor")) {
        threadsToDestination.merge(member, -1, Integer::sum);
      }
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ", maxThreadsToDestination: " + maxThreadsToDestination;
  }
}
