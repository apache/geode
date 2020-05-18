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
 *
 */

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;

@SuppressWarnings("unchecked")
public class CheckPrimaryBucketFunction implements Function {
  private static final CountDownLatch latch = new CountDownLatch(1);

  public static void awaitLatch() {
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContextImpl regionFunctionContext = (RegionFunctionContextImpl) context;
    String key = (String) regionFunctionContext.getFilter().iterator().next();

    ResultSender result = context.getResultSender();
    DistributedMember member = context.getCache().getDistributedSystem().getDistributedMember();

    assertThat(isMemberPrimary(regionFunctionContext, key, member)).isTrue();

    latch.countDown();

    isMemberPrimary(regionFunctionContext, key, member);

    GeodeAwaitility.await()
        .during(10, TimeUnit.SECONDS)
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> !isMemberPrimary(regionFunctionContext, key, member));

    result.lastResult(true);
  }

  private boolean isMemberPrimary(RegionFunctionContextImpl context, String key,
      DistributedMember member) {
    DistributedMember primaryForKey = PartitionRegionHelper
        .getPrimaryMemberForKey(context.getDataSet(), key);

    boolean primaryness = primaryForKey.equals(member);
    System.err.println("--->>> " + primaryness);

    return primaryness;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public String getId() {
    return CheckPrimaryBucketFunction.class.getName();
  }
}
