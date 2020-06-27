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

package org.apache.geode.redis.internal.executor.key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisKeyInRegion;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.executor.SingleResultCollector;
import org.apache.geode.redis.internal.executor.StripedExecutor;

@SuppressWarnings("unchecked")
public class RenameFunction implements InternalFunction {

  public static final String ID = "REDIS_RENAME_FUNCTION";

  private final transient StripedExecutor stripedExecutor;
  private final RedisStats redisStats;

  public static void register(StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    FunctionService.registerFunction(new RenameFunction(stripedExecutor, redisStats));
  }


  public RenameFunction(StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    this.stripedExecutor = stripedExecutor;
    this.redisStats = redisStats;
  }

  @Override
  public void execute(FunctionContext context) {
    RenameContext renameContext = new RenameContext(context);
    LocalDataSet localDataSet = (LocalDataSet) renameContext.getLocalDataSet();

    if (renameContext.getKeysFixedOnPrimary().size() < 2) {
      Runnable computation = () -> {
        boolean result = fixKeysOnPrimary(renameContext);
        context.getResultSender().lastResult(result);
      };

      computeWithPrimaryLocked(renameContext.getKeyToLock(),
          localDataSet, computation);
    } else {
      Object result = acquireLockIfNeeded(renameContext);
      context.getResultSender().lastResult(result);
    }
  }

  public static void computeWithPrimaryLocked(Object key, LocalDataSet localDataSet, Runnable r) {
    PartitionedRegion partitionedRegion = localDataSet.getProxy();

    partitionedRegion.computeWithPrimaryLocked(key, r);
  }

  private boolean fixKeysOnPrimary(RenameContext context) {
    context.getKeysFixedOnPrimary().add(context.getKeyToLock());
    context.getKeysToOperateOn().remove(context.getKeyToLock());

    if (!context.getKeysToOperateOn().isEmpty()) {
      return getLockForNextKey(context);
    }

    List<ByteArrayWrapper> keysToOperateOn = new ArrayList<>(context.getKeysFixedOnPrimary());
    context.getKeysToOperateOn().addAll(keysToOperateOn);

    context.getKeysToOperateOn().sort(((o1, o2) -> compare(o1, o2, context)));

    return getLockForNextKey(context);
  }

  private int compare(Object object1, Object object2, RenameContext context) {
    int result = stripedExecutor.compareStripes(object1, object2);
    if (result == 0) {
      DistributedMember distributedMember1 =
          PartitionRegionHelper.getPrimaryMemberForKey(context.getDataRegion(), object1);
      DistributedMember distributedMember2 =
          PartitionRegionHelper.getPrimaryMemberForKey(context.getDataRegion(), object2);

      result = distributedMember1.compareTo(distributedMember2);
    }

    return result;
  }

  private boolean acquireLockIfNeeded(RenameContext context) {
    if (isLockNeededForCurrentKey(context)) {
      return stripedExecutor
          .execute(context.getKeyToLock(),
              () -> renameOrGetLockForNextKey(context));
    } else {
      return renameOrGetLockForNextKey(context);
    }
  }

  private boolean isLockNeededForCurrentKey(RenameContext context) {
    return context
        .getLockedKeys()
        .stream()
        .noneMatch(
            (lockedKey) -> alreadyHaveLockForCurrentKey(lockedKey, context));
  }

  private boolean renameOrGetLockForNextKey(RenameContext context) {
    markCurrentKeyAsLocked(context);

    if (allKeysHaveLocks(context)) {
      return rename(context);
    } else {
      return getLockForNextKey(context);
    }
  }

  private boolean allKeysHaveLocks(RenameContext context) {
    return context.getKeysToOperateOn().isEmpty();
  }

  private boolean rename(RenameContext context) {
    return new RedisKeyInRegion(context.getDataRegion(), redisStats)
        .rename(context.getOldKey(), context.getNewKey());
  }

  private void markCurrentKeyAsLocked(RenameContext context) {
    ByteArrayWrapper keyToMarkAsLocked = context.getKeyToLock();

    context.getLockedKeys().add(keyToMarkAsLocked);
    context.getKeysToOperateOn().remove(keyToMarkAsLocked);
  }

  private boolean alreadyHaveLockForCurrentKey(
      ByteArrayWrapper lockedKey, RenameContext context) {

    boolean stripesAreTheSame =
        stripedExecutor.compareStripes(lockedKey, context.getKeyToLock()) == 0;

    if (!stripesAreTheSame) {
      return false;
    }

    Region region = context.getDataRegion();

    DistributedMember primaryMemberForCurrentKey =
        PartitionRegionHelper
            .getPrimaryMemberForKey(region, context.getKeyToLock());

    DistributedMember primaryMemberForLockedKey = PartitionRegionHelper
        .getPrimaryMemberForKey(region, lockedKey);

    boolean primaryMembersAreTheSame =
        primaryMemberForCurrentKey.equals(primaryMemberForLockedKey);

    return primaryMembersAreTheSame;
  }

  private boolean getLockForNextKey(RenameContext context) {

    SingleResultCollector<Object> rc = new SingleResultCollector<>();

    FunctionService
        .onRegion(context.getDataRegion())
        .withFilter(Collections.singleton(context.getKeysToOperateOn().get(0)))
        .setArguments(
            new Object[] {context.getOldKey(),
                context.getNewKey(),
                context.getKeysToOperateOn(),
                context.getKeysFixedOnPrimary(),
                context.getLockedKeys()})
        .withCollector(rc)
        .execute(RenameFunction.ID)
        .getResult();

    return (boolean) rc.getResult();
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  private static class RenameContext {

    private final RegionFunctionContextImpl context;

    public RenameContext(FunctionContext context) {
      this.context = (RegionFunctionContextImpl) context;
    }

    private ByteArrayWrapper getOldKey() {
      return (ByteArrayWrapper) ((Object[]) context.getArguments())[0];
    }

    private ByteArrayWrapper getNewKey() {
      return (ByteArrayWrapper) ((Object[]) context.getArguments())[1];
    }

    private List<ByteArrayWrapper> getKeysToOperateOn() {
      return (List<ByteArrayWrapper>) ((Object[]) context.getArguments())[2];
    }

    private List<ByteArrayWrapper> getKeysFixedOnPrimary() {
      return (List<ByteArrayWrapper>) ((Object[]) context.getArguments())[3];
    }

    private List<ByteArrayWrapper> getLockedKeys() {
      return (List<ByteArrayWrapper>) ((Object[]) context.getArguments())[4];
    }


    private ByteArrayWrapper getKeyToLock() {
      return (ByteArrayWrapper) context.getFilter().iterator().next();
    }

    private Region getDataRegion() {
      return context.getDataSet();
    }

    private Region<ByteArrayWrapper, RedisSet> getLocalDataSet() {
      return context.getLocalDataSet(getDataRegion());
    }
  }
}
