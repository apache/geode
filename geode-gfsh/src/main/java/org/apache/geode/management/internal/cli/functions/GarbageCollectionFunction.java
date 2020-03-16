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
package org.apache.geode.management.internal.cli.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.cli.util.BytesToString;

/**
 *
 * Class for Garbage collection function
 *
 *
 *
 */
public class GarbageCollectionFunction implements InternalFunction<Void> {
  public static final String ID = GarbageCollectionFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext<Void> context) {
    BytesToString bytesToString = new BytesToString();

    Map<String, String> resultMap = null;
    try {
      Cache cache = context.getCache();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();
      long freeMemoryBeforeGC = Runtime.getRuntime().freeMemory();
      long totalMemoryBeforeGC = Runtime.getRuntime().totalMemory();
      long timeBeforeGC = System.currentTimeMillis();
      Runtime.getRuntime().gc();

      long freeMemoryAfterGC = Runtime.getRuntime().freeMemory();
      long totalMemoryAfterGC = Runtime.getRuntime().totalMemory();
      long timeAfterGC = System.currentTimeMillis();

      resultMap = new HashMap<>();
      resultMap.put("MemberId", member.getId());
      resultMap.put("HeapSizeBeforeGC", bytesToString.of(totalMemoryBeforeGC - freeMemoryBeforeGC));
      resultMap.put("HeapSizeAfterGC", bytesToString.of(totalMemoryAfterGC - freeMemoryAfterGC));
      resultMap.put("TimeSpentInGC", String.valueOf(timeAfterGC - timeBeforeGC));
    } catch (Exception ex) {
      String message = "Exception in GC:" + ex.getMessage() + ExceptionUtils.getStackTrace(ex);

      context.getResultSender().lastResult(message);
    }
    context.getResultSender().lastResult(resultMap);
  }

  @Override
  public String getId() {
    return GarbageCollectionFunction.ID;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
