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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;

@SuppressWarnings("unchecked")
public class CheckPrimaryBucketFunction implements Function {
  public static final String ID = CheckPrimaryBucketFunction.class.getName();
  private final CountDownLatch signalFunctionHasStarted = new CountDownLatch(1);
  private final CountDownLatch signalPrimaryHasMoved = new CountDownLatch(1);
  private static final Logger logger = LogService.getLogger();

  public void waitForFunctionToStart() {
    try {
      signalFunctionHasStarted.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void finishedMovingPrimary() {
    signalPrimaryHasMoved.countDown();
  }

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContextImpl regionFunctionContext = (RegionFunctionContextImpl) context;
    String key = (String) regionFunctionContext.getFilter().iterator().next();
    boolean releaseLatchEarly = (boolean) context.getArguments();

    ResultSender result = context.getResultSender();
    DistributedMember member = context.getCache().getDistributedSystem().getDistributedMember();

    if (!isMemberPrimary(regionFunctionContext, key, member)) {
      LogService.getLogger().error("Member is not primary.");
      result.lastResult(false);
      return;
    }

    Region<?, ?> localRegion =
        regionFunctionContext.getLocalDataSet(regionFunctionContext.getDataSet());

    if (releaseLatchEarly) {
      signalFunctionHasStarted.countDown();
      // now wait until test has moved primary
      try {
        signalPrimaryHasMoved.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    Runnable r = () -> {
      if (!releaseLatchEarly) {
        signalFunctionHasStarted.countDown();
      }

      try {
        GeodeAwaitility.await()
            .during(10, TimeUnit.SECONDS)
            .atMost(11, TimeUnit.SECONDS)
            .until(() -> isMemberPrimary(regionFunctionContext, key, member));
        result.lastResult(true);
      } catch (Exception e) {
        e.printStackTrace();
        result.lastResult(false);
      }
    };

    LocalDataSet localDataSet = (LocalDataSet) localRegion;
    PartitionedRegion partitionedRegion = localDataSet.getProxy();
    try {
      partitionedRegion.computeWithPrimaryLocked(key, r);
    } catch (PrimaryBucketLockException ex) {
      throw new BucketMovedException(ex.toString());
    }
  }

  private boolean isMemberPrimary(RegionFunctionContextImpl context, String key,
      DistributedMember member) {
    DistributedMember primaryForKey = PartitionRegionHelper
        .getPrimaryMemberForKey(context.getDataSet(), key);

    return primaryForKey.equals(member);
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public String getId() {
    return ID;
  }
}
