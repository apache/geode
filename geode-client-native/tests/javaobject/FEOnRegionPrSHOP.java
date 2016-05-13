/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.execute.PartitionedRegionFunctionResultSender;

import java.util.Properties;

public class FEOnRegionPrSHOP extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {   
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    PartitionedRegionFunctionResultSender rs = (PartitionedRegionFunctionResultSender)regionContext.getResultSender();	
	ResultSender sender = context.getResultSender();	  
    sender.lastResult(rs.isLocallyExecuted());	 //This is single hop indication.
  }

  public String getId() {
    return "FEOnRegionPrSHOP";
  }
  
  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties arg0) {
  }
}
