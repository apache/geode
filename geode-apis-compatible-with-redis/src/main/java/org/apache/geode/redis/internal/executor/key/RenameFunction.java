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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.data.CommandHelper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisKeyCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.SingleResultCollector;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class RenameFunction implements InternalFunction<RedisKey> {

  public static final String ID = "REDIS_RENAME_FUNCTION";
  private static final long serialVersionUID = 7047473969356686453L;

  private final transient PartitionedRegion partitionedRegion;
  private final transient CommandHelper commandHelper;
  private final transient RedisKeyCommandsFunctionExecutor keyCommands;

  public static void register(Region<RedisKey, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    FunctionService.registerFunction(new RenameFunction(dataRegion, stripedExecutor, redisStats));
  }

  public RenameFunction(Region<RedisKey, RedisData> dataRegion,
      StripedExecutor stripedExecutor,
      RedisStats redisStats) {
    partitionedRegion = (PartitionedRegion) dataRegion;
    commandHelper = new CommandHelper(dataRegion, redisStats, stripedExecutor);
    keyCommands = new RedisKeyCommandsFunctionExecutor(commandHelper);
  }

  @Override
  public void execute(FunctionContext<RedisKey> context) {
    RenameContext renameContext = new RenameContext(context);

    if (renameContext.getKeysFixedOnPrimary().size() < 2) {
      Runnable computation = () -> {
        boolean result = fixKeysOnPrimary(renameContext);
        context.getResultSender().lastResult(result);
      };

      partitionedRegion.computeWithPrimaryLocked(renameContext.getKeyToLock(), computation);

    } else {
      Object result = acquireLockIfNeeded(renameContext);
      context.getResultSender().lastResult(result);
    }
  }

  private boolean fixKeysOnPrimary(RenameContext context) {
    context.getKeysFixedOnPrimary().add(context.getKeyToLock());
    context.getKeysToOperateOn().remove(context.getKeyToLock());

    if (!context.getKeysToOperateOn().isEmpty()) {
      return getLockForNextKey(context);
    }

    List<RedisKey> keysToOperateOn = new ArrayList<>(context.getKeysFixedOnPrimary());
    context.getKeysToOperateOn().addAll(keysToOperateOn);

    context.getKeysToOperateOn().sort(((o1, o2) -> compare(o1, o2, context)));

    return getLockForNextKey(context);
  }

  private StripedExecutor getStripedExecutor() {
    return commandHelper.getStripedExecutor();
  }

  private int compare(RedisKey object1, RedisKey object2, RenameContext context) {
    int result = getStripedExecutor().compareStripes(object1, object2);
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
      return getStripedExecutor()
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
    return keyCommands.rename(context.getOldKey(), context.getNewKey());
  }

  private void markCurrentKeyAsLocked(RenameContext context) {
    RedisKey keyToMarkAsLocked = context.getKeyToLock();

    context.getLockedKeys().add(keyToMarkAsLocked);
    context.getKeysToOperateOn().remove(keyToMarkAsLocked);
  }

  private boolean alreadyHaveLockForCurrentKey(
      RedisKey lockedKey, RenameContext context) {

    boolean stripesAreTheSame =
        getStripedExecutor().compareStripes(lockedKey, context.getKeyToLock()) == 0;

    if (!stripesAreTheSame) {
      return false;
    }

    Region<RedisKey, RedisData> region = context.getDataRegion();

    DistributedMember primaryMemberForCurrentKey =
        PartitionRegionHelper
            .getPrimaryMemberForKey(region, context.getKeyToLock());

    DistributedMember primaryMemberForLockedKey = PartitionRegionHelper
        .getPrimaryMemberForKey(region, lockedKey);

    return primaryMemberForCurrentKey.equals(primaryMemberForLockedKey);
  }

  private boolean getLockForNextKey(RenameContext context) {

    SingleResultCollector<Boolean> rc = new SingleResultCollector<>();

    Execution<Object[], Boolean, Boolean> execution =
        uncheckedCast(FunctionService.onRegion(context.getDataRegion()));
    execution.withFilter(Collections.singleton(context.getKeysToOperateOn().get(0)))
        .setArguments(
            new Object[] {context.getOldKey(),
                context.getNewKey(),
                context.getKeysToOperateOn(),
                context.getKeysFixedOnPrimary(),
                context.getLockedKeys()})
        .withCollector(rc)
        .execute(RenameFunction.ID)
        .getResult();

    return rc.getResult();
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }


  private static class RenameContext {

    private final RegionFunctionContextImpl context;

    public RenameContext(FunctionContext<RedisKey> context) {
      this.context = (RegionFunctionContextImpl) context;
    }

    private RedisKey getOldKey() {
      return (RedisKey) ((Object[]) context.getArguments())[0];
    }

    private RedisKey getNewKey() {
      return (RedisKey) ((Object[]) context.getArguments())[1];
    }

    private List<RedisKey> getKeysToOperateOn() {
      return uncheckedCast(((Object[]) context.getArguments())[2]);
    }

    private List<RedisKey> getKeysFixedOnPrimary() {
      return uncheckedCast(((Object[]) context.getArguments())[3]);
    }

    private List<RedisKey> getLockedKeys() {
      return uncheckedCast(((Object[]) context.getArguments())[4]);
    }


    private RedisKey getKeyToLock() {
      return (RedisKey) context.getFilter().iterator().next();
    }

    private Region<RedisKey, RedisData> getDataRegion() {
      return context.getDataSet();
    }
  }
}
