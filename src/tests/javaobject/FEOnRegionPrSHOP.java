/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;

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
