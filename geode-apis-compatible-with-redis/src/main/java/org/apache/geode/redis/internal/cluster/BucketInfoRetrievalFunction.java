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

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKey;

public class BucketInfoRetrievalFunction implements InternalFunction<Void> {

  public static final String ID = BucketInfoRetrievalFunction.class.getName();
  private final String hostAddress;
  private final int redisPort;

  private BucketInfoRetrievalFunction(String address, int redisPort) {
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
    FunctionService.registerFunction(new BucketInfoRetrievalFunction(address, redisPort));
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    Region<RedisKey, ByteArrayWrapper> region =
        context.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);

    String memberId =
        context.getCache().getDistributedSystem().getDistributedMember().getUniqueId();

    MemberBuckets mb = new MemberBuckets(memberId, hostAddress, redisPort,
        getLocalPrimaryBucketIds(region));
    context.getResultSender().lastResult(mb);
  }

  @Override
  public String getId() {
    return ID;
  }

  @SuppressWarnings("unchecked")
  public static Set<Integer> getLocalPrimaryBucketIds(Region region) {
    PartitionedRegion pr = (PartitionedRegion) region;
    if (pr.getDataStore() != null) {
      return new HashSet<>(pr.getDataStore().getAllLocalPrimaryBucketIds());
    } else {
      return Collections.EMPTY_SET;
    }
  }

  public static class MemberBuckets implements Serializable {
    private final String memberId;
    private final String hostAddress;
    private final int port;
    private final Set<Integer> primaryBucketIds;

    public MemberBuckets(String memberId, String hostAddress, int port,
        Set<Integer> primaryBucketIds) {
      this.memberId = memberId;
      this.hostAddress = hostAddress;
      this.port = port;
      this.primaryBucketIds = primaryBucketIds;
    }

    public String getMemberId() {
      return memberId;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getPort() {
      return port;
    }

    public Set<Integer> getPrimaryBucketIds() {
      return primaryBucketIds;
    }

  }
}
