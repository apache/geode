/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.functions;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

@SuppressWarnings("serial")
public class DistributedRegionFunction extends FunctionAdapter {

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext rcontext = (RegionFunctionContext)context;
    Region<Object, Object> region = rcontext.getDataSet();
    InternalDistributedSystem sys = InternalDistributedSystem
        .getConnectedInstance();
    sys.getLogWriter().fine(
        "DistributedRegionFunction#execute( " + rcontext + " )");
    Assert.assertTrue(region.getAttributes().getDataPolicy().withStorage());
    Assert.assertTrue(region.getAttributes()
        .getDataPolicy() != DataPolicy.NORMAL);
    Assert.assertTrue(rcontext.getFilter().size() == 20);
    long startTime = System.currentTimeMillis();
    // Boolean.TRUE dummy argument indicates that CacheClose has to be done from
    // the body itself
    if (Boolean.TRUE.equals(rcontext.getArguments())) {
      // do not close cache in retry
      if (!rcontext.isPossibleDuplicate()) {
        sys.disconnect();
        throw new CacheClosedException("Throwing CacheClosedException "
            + "to simulate failover during function exception");
      }
    }
    else {
      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return false;
        }

        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 12000, 500, false);
    }
    long endTime = System.currentTimeMillis();

    // intentionally doing region operation to cause cacheClosedException
    region.put("execKey-201", new Integer(201));

    if (rcontext.isPossibleDuplicate()) { // Below operation is done when the
                                          // function is reexecuted
      region.put("execKey-202", new Integer(202));
      region.put("execKey-203", new Integer(203));
    }
    sys.getLogWriter().fine(
        "Time wait for Function Execution = " + (endTime - startTime));
    for (int i = 0; i < 5000; i++) {
      context.getResultSender().sendResult(Boolean.TRUE);
    }
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  @Override
  public String getId() {
    return "DistributedRegionFunction";
  }
  
  @Override
  public boolean isHA() {
    return true;
  }
}
