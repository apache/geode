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
import java.util.Set;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.inet.LocalHostUtil;

public class BucketRetrievalFunction implements Function<Void> {

  private static final String hostAddress;

  static {
    InetAddress localhost = null;
    try {
      localhost = LocalHostUtil.getLocalHost();
    } catch (Exception ex) {
    }

    hostAddress = localhost == null ? "localhost" : localhost.getHostAddress();
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    LocalDataSet local = (LocalDataSet) PartitionRegionHelper
        .getLocalDataForContext((RegionFunctionContext) context);

    MemberBuckets mb = new MemberBuckets(hostAddress, local.getBucketSet());
    context.getResultSender().lastResult(mb);
  }

  public static class MemberBuckets implements Serializable {
    private final String hostAddress;
    private final Set<Integer> bucketIds;

    public MemberBuckets(String hostAddress, Set<Integer> bucketIds) {
      this.hostAddress = hostAddress;
      this.bucketIds = bucketIds;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public Set<Integer> getBucketIds() {
      return bucketIds;
    }
  }
}
