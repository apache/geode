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
package org.apache.geode.internal.cache.execute;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

public class PerformanceTestFunction extends FunctionAdapter {

  public PerformanceTestFunction() {}

  /**
   * Application execution implementation
   *
   * @since GemFire 5.8Beta
   */
  @Override
  public void execute(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext prContext = (RegionFunctionContext) context;
      final Set allKeysSet = prContext.getFilter();

      ArrayList vals = new ArrayList();
      Region fcd = PartitionRegionHelper.getLocalDataForContext(prContext);
      for (final Object o : allKeysSet) {
        Object val = fcd.get(o);
        assertNotNull(val);
        vals.add(val);
      }
      context.getResultSender().lastResult(vals);
    } else {
      context.getResultSender().lastResult(null);
    }
  }

  /**
   * Get the function identifier, used by clients to invoke this function
   *
   * @return an object identifying this function
   * @since GemFire 5.8Beta
   */
  @Override
  public String getId() {
    return getClass().getName();
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
