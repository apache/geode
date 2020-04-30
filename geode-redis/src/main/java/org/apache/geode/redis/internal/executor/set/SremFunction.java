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

package org.apache.geode.redis.internal.executor.set;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.ByteArrayWrapper;

class SremFunction implements Function<ArrayList<ByteArrayWrapper>> {

  public static final String ID = "SREM_FUNCTION";

  @SuppressWarnings("unchecked")
  @Override
  public void execute(FunctionContext<ArrayList<ByteArrayWrapper>> context) {
    RegionFunctionContextImpl regionFunctionContext =
        (RegionFunctionContextImpl) context;
    ByteArrayWrapper key =
        (ByteArrayWrapper) regionFunctionContext.getFilter().iterator().next();
    Region<ByteArrayWrapper, DeltaSet> localRegion =
        regionFunctionContext.getLocalDataSet(regionFunctionContext.getDataSet());
    ArrayList<ByteArrayWrapper> membersToRemove =
        (ArrayList<ByteArrayWrapper>) regionFunctionContext.getArguments();
    AtomicBoolean setWasDeleted = new AtomicBoolean();
    long membersRemoved = DeltaSet.srem(localRegion, key, membersToRemove, setWasDeleted);
    ResultSender<Long> resultSender = regionFunctionContext.getResultSender();
    resultSender.sendResult(membersRemoved);
    resultSender.lastResult(setWasDeleted.get() ? 1L : 0L);
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public String getId() {
    return ID;
  }
}
