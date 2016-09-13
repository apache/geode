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
package com.gemstone.gemfire.internal.cache.execute;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class PerformanceTestFunction extends FunctionAdapter {

  public PerformanceTestFunction() {
  }

  /**
   *  Application execution implementation
   *  @since GemFire 5.8Beta
   */
  public void execute(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      final Set allKeysSet = prContext.getFilter();

      ArrayList vals = new ArrayList();
      Region fcd = PartitionRegionHelper.getLocalDataForContext(prContext);
      for (Iterator i = allKeysSet.iterator(); i.hasNext();) {
        Object val = fcd.get(i.next());
        assertNotNull(val);
        vals.add(val);
      }
      context.getResultSender().lastResult(vals);
    }
    else {
      context.getResultSender().lastResult( null);
    }
  }

  /**
   * Get the function identifier, used by clients to invoke this function
   * @return an object identifying this function
   * @since GemFire 5.8Beta
   */
  public String getId() {
    return getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return false;
  }
}
