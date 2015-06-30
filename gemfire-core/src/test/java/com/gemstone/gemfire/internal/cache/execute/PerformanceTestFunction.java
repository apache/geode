/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import junit.framework.Assert;

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
   *  @since 5.8Beta
   */
  public void execute(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      final Set allKeysSet = prContext.getFilter();

      ArrayList vals = new ArrayList();
      Region fcd = PartitionRegionHelper.getLocalDataForContext(prContext);
      for (Iterator i = allKeysSet.iterator(); i.hasNext();) {
        Object val = fcd.get(i.next());
        Assert.assertNotNull(val);
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
   * @since 5.8Beta
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
